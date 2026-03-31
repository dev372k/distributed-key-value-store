use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Write, BufRead, BufReader};
use std::sync::{Arc, Mutex};
use warp::Filter;
use std::env;
use tokio::time::{timeout, Duration};

#[derive(Clone)]
struct KvStore {
    store: Arc<Mutex<HashMap<String, String>>>,
    log_file: String,
    replicas: Vec<String>,
}

impl KvStore {
    fn new(log_file: &str, replicas: Vec<String>) -> Self {
        let kv = KvStore {
            store: Arc::new(Mutex::new(HashMap::new())),
            log_file: log_file.to_string(),
            replicas,
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
                        Some(&"PUT") if parts.len() == 3 => {
                            store.insert(parts[1].to_string(), parts[2].to_string());
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
        {
            let mut store = self.store.lock().unwrap_or_else(|e| e.into_inner());
            store.insert(key.clone(), value.clone());
            self.log(&format!("PUT {} {}", key, value));
        } // ✅ lock DROPPED here

        // now safe
        self.replicate(&key, &value).await;
    }

    fn get(&self, key: String) -> Option<String> {
        let store = self.store.lock().unwrap_or_else(|e| e.into_inner());
        store.get(&key).cloned()
    }

    fn delete(&self, key: String) {
        let mut store = self.store.lock().unwrap_or_else(|e| e.into_inner());
        store.remove(&key);
        self.log(&format!("DELETE {}", key));
    }

    async fn replicate(&self, key: &str, value: &str) {
        for replica in &self.replicas {
            let url = format!("{}/replicate?key={}&value={}", replica, key, value);

            let result = timeout(Duration::from_secs(2), reqwest::get(&url)).await;

            match result {
                Ok(Ok(_)) => {
                    println!("Replicated to {}", replica);
                }
                _ => {
                    println!("Failed to replicate to {}", replica);
                }
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    let port: u16 = if args.len() > 1 {
        args[1].parse().expect("Invalid port")
    } else {
        3030
    };

    // Separate log file per node (EBS-ready)
    let log_file = format!("data_{}.log", port);

    // Replica configuration
    let replicas = if port == 3030 {
        // vec!["http://127.0.0.1:3031".to_string()]
        vec!["3.238.88.60:3031".to_string()]
    } else {
        // vec!["http://127.0.0.1:3030".to_string()]
        vec!["3.229.120.130:3030".to_string()]
    };

    let kv = KvStore::new(&log_file, replicas);
    let kv_filter = warp::any().map(move || kv.clone());

    // PUT
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

    // GET
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

    // DELETE
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

    // REPLICATE (internal use only)
    let replicate = warp::path("replicate")
        .and(warp::query::<HashMap<String, String>>())
        .and(kv_filter.clone())
        .map(|params: HashMap<String, String>, kv: KvStore| {
            if let (Some(key), Some(value)) = (params.get("key"), params.get("value")) {
                let mut store = kv.store.lock().unwrap_or_else(|e| e.into_inner());
                store.insert(key.clone(), value.clone());
                kv.log(&format!("PUT {} {}", key, value));
                format!("Replicated {}={}", key, value)
            } else {
                "Missing key/value".to_string()
            }
        });

    let routes = put.or(get).or(delete).or(replicate);

    println!("Server running on port {}", port);

    warp::serve(routes).run(([0, 0, 0, 0], port)).await;
}