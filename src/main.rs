use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Write, BufRead, BufReader};
use std::sync::{Arc, Mutex};
use warp::Filter;
use std::env;
use tokio::time::{timeout, Duration};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone)]
struct KvStore {
    store: Arc<Mutex<HashMap<String, (String, u128)>>>,
    log_file: String,
    replicas: Vec<String>,
    port: u16,
}

impl KvStore {
    fn new(log_file: &str, replicas: Vec<String>, port: u16) -> Self {
        let kv = KvStore {
            store: Arc::new(Mutex::new(HashMap::new())),
            log_file: log_file.to_string(),
            replicas,
            port,
        };

        kv.load();
        kv
    }

    fn load(&self) {
        let file = OpenOptions::new().read(true).open(&self.log_file);

        if let Ok(file) = file {
            let reader = BufReader::new(file);
            let mut store = self.store.lock().unwrap_or_else(|e| e.into_inner());

            for line in reader.lines() {
                if let Ok(cmd) = line {
                    let parts: Vec<&str> = cmd.split_whitespace().collect();

                    match parts.get(0) {
                        Some(&"PUT") if parts.len() == 4 => {
                            let ts: u128 = parts[3].parse().unwrap_or(0);
                            store.insert(parts[1].to_string(), (parts[2].to_string(), ts));
                        }
                        Some(&"DELETE") if parts.len() == 2 => {
                            store.remove(parts[1]);
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    fn log(&self, command: &str) {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_file)
            .unwrap();

        writeln!(file, "{}", command).unwrap();
    }

    async fn put(&self, key: String, value: String) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        {
            let mut store = self.store.lock().unwrap_or_else(|e| e.into_inner());

            match store.get(&key) {
                Some((_, existing_ts)) if *existing_ts > timestamp => {
                    println!("[Node {}] Ignored older write for key={}", self.port, key);
                    return;
                }
                _ => {
                    store.insert(key.clone(), (value.clone(), timestamp));
                    self.log(&format!("PUT {} {} {}", key, value, timestamp));

                    println!(
                        "[Node {}] PUT key={} value={} ts={}",
                        self.port, key, value, timestamp
                    );
                }
            }
        }

        self.replicate(&key, &value, timestamp).await;
    }

    fn get(&self, key: String) -> Option<String> {
        let store = self.store.lock().unwrap_or_else(|e| e.into_inner());
        store.get(&key).map(|(v, _)| v.clone())
    }

    fn delete(&self, key: String) {
        let mut store = self.store.lock().unwrap_or_else(|e| e.into_inner());
        store.remove(&key);
        self.log(&format!("DELETE {}", key));

        println!("[Node {}] DELETE key={}", self.port, key);
    }

    async fn replicate(&self, key: &str, value: &str, timestamp: u128) {
        for replica in &self.replicas {
            println!("[Node {}] Replicating to {}", self.port, replica);

            let url = format!(
                "{}/replicate?key={}&value={}&ts={}",
                replica, key, value, timestamp
            );

            let result = timeout(Duration::from_secs(2), reqwest::get(&url)).await;

            match result {
                Ok(Ok(_)) => println!("[Node {}] Replicated to {}", self.port, replica),
                _ => println!("[Node {}] Failed to replicate to {}", self.port, replica),
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        println!("Usage: {} <port> [replica1 replica2 ...]", args[0]);
        return;
    }

    let port: u16 = args[1].parse().expect("Invalid port");

    let log_file = format!("data_{}.log", port);

    let replicas: Vec<String> = if args.len() > 2 {
        args[2..].to_vec()
    } else {
        vec![]
    };

    println!("[Node {}] Replicas: {:?}", port, replicas);

    let kv = KvStore::new(&log_file, replicas, port);

    let kv_for_filter = kv.clone();
    let kv_clone = kv.clone();

    let kv_filter = warp::any().map(move || kv_for_filter.clone());

    let put = warp::path("put")
        .and(warp::query::<HashMap<String, String>>())
        .and(kv_filter.clone())
        .and_then(|params: HashMap<String, String>, kv: KvStore| async move {
            if let (Some(key), Some(value)) = (params.get("key"), params.get("value")) {
                kv.put(key.clone(), value.clone()).await;
                Ok::<_, warp::Rejection>(format!("Stored {}={}", key, value))
            } else {
                Ok("Missing key/value".to_string())
            }
        });

    let get = warp::path("get")
        .and(warp::query::<HashMap<String, String>>())
        .and(kv_filter.clone())
        .map(|params: HashMap<String, String>, kv: KvStore| {
            if let Some(key) = params.get("key") {
                match kv.get(key.clone()) {
                    Some(val) => format!("Value: {}", val),
                    None => "Not found".to_string(),
                }
            } else {
                "Missing key".to_string()
            }
        });

    let delete = warp::path("delete")
        .and(warp::query::<HashMap<String, String>>())
        .and(kv_filter.clone())
        .map(|params: HashMap<String, String>, kv: KvStore| {
            if let Some(key) = params.get("key") {
                kv.delete(key.clone());
                format!("Deleted {}", key)
            } else {
                "Missing key".to_string()
            }
        });

    let replicate = warp::path("replicate")
        .and(warp::query::<HashMap<String, String>>())
        .and(kv_filter.clone())
        .map(|params: HashMap<String, String>, kv: KvStore| {
            if let (Some(key), Some(value), Some(ts)) =
                (params.get("key"), params.get("value"), params.get("ts"))
            {
                let timestamp: u128 = ts.parse().unwrap_or(0);

                let mut store = kv.store.lock().unwrap_or_else(|e| e.into_inner());

                match store.get(key) {
                    Some((_, existing_ts)) if *existing_ts > timestamp => {}
                    _ => {
                        store.insert(key.clone(), (value.clone(), timestamp));
                        kv.log(&format!("PUT {} {} {}", key, value, timestamp));

                        println!(
                            "[Node {}] Received replication key={} value={}",
                            kv.port, key, value
                        );
                    }
                }

                format!("Replicated {}={}", key, value)
            } else {
                "Missing key/value".to_string()
            }
        });

    let all = warp::path("all")
        .and(kv_filter.clone())
        .map(|kv: KvStore| {
            let store = kv.store.lock().unwrap_or_else(|e| e.into_inner());
            serde_json::to_string(&*store).unwrap()
        });

    let routes = put.or(get).or(delete).or(replicate).or(all);

    tokio::spawn(async move {
        loop {
            for replica in &kv_clone.replicas {
                let url = format!("{}/all", replica);

                println!("[Node {}] Syncing from {}", kv_clone.port, replica);

                if let Ok(resp) = reqwest::get(&url).await {
                    if let Ok(text) = resp.text().await {
                        if let Ok(remote_store) = serde_json::from_str::<
                            HashMap<String, (String, u128)>
                        >(&text)
                        {
                            let mut local =
                                kv_clone.store.lock().unwrap_or_else(|e| e.into_inner());

                            for (key, (value, ts)) in remote_store {
                                match local.get(&key) {
                                    Some((_, local_ts)) if *local_ts >= ts => {}
                                    Some(_) => {
                                        local.insert(key.clone(), (value.clone(), ts));
                                        println!(
                                            "[Node {}] Updated key={} value={}",
                                            kv_clone.port, key, value
                                        );
                                    }
                                    None => {
                                        local.insert(key.clone(), (value.clone(), ts));
                                        println!(
                                            "[Node {}] Added key={} value={}",
                                            kv_clone.port, key, value
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }

            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });

    println!("[Node {}] Server running", port);

    warp::serve(routes).run(([0, 0, 0, 0], port)).await;
}