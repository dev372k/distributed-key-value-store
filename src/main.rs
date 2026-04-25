use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Write, BufRead, BufReader};
use std::sync::{Arc, Mutex};
use warp::Filter;
use std::env;
use tokio::time::{timeout, Duration};
use std::time::{SystemTime, UNIX_EPOCH};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

// ---------------- HASH FUNCTION ----------------
fn hash_str(input: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    input.hash(&mut hasher);
    hasher.finish()
}

#[derive(Clone)]
struct KvStore {
    store: Arc<Mutex<HashMap<String, (String, u128)>>>,
    log_file: String,
    nodes: Vec<String>,   // all nodes
    port: u16,
    node_id: u64,
}

impl KvStore {
    fn new(log_file: &str, nodes: Vec<String>, port: u16) -> Self {
        let address = format!("http://127.0.0.1:{}", port);
        let node_id = hash_str(&address);

        let kv = KvStore {
            store: Arc::new(Mutex::new(HashMap::new())),
            log_file: log_file.to_string(),
            nodes,
            port,
            node_id,
        };

        kv.load();
        kv
    }

    fn load(&self) {
        if let Ok(file) = OpenOptions::new().read(true).open(&self.log_file) {
            let reader = BufReader::new(file);
            let mut store = self.store.lock().unwrap_or_else(|e| e.into_inner());

            for line in reader.lines().flatten() {
                let parts: Vec<&str> = line.split_whitespace().collect();

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

    fn log(&self, command: &str) {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_file)
            .unwrap();

        writeln!(file, "{}", command).unwrap();
    }

    // ---------------- CONSISTENT HASHING ----------------
    fn find_node(&self, key: &str) -> String {
        let mut ring: Vec<(u64, String)> = self
            .nodes
            .iter()
            .map(|n| (hash_str(n), n.clone()))
            .collect();

        ring.sort_by_key(|k| k.0);

        let key_hash = hash_str(key);

        for (node_hash, node) in &ring {
            if *node_hash >= key_hash {
                return node.clone();
            }
        }

        ring[0].1.clone()
    }

    // ---------------- PUT WITH ROUTING ----------------
    async fn put(&self, key: String, value: String) {
        let responsible = self.find_node(&key);
        let my_address = format!("http://127.0.0.1:{}", self.port);

        if responsible != my_address {
            println!(
                "[Node {}] Forwarding key={} to {}",
                self.port, key, responsible
            );

            let url = format!("{}/put?key={}&value={}", responsible, key, value);
            let _ = reqwest::get(&url).await;
            return;
        }

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        {
            let mut store = self.store.lock().unwrap_or_else(|e| e.into_inner());

            match store.get(&key) {
                Some((_, existing_ts)) if *existing_ts > timestamp => return,
                _ => {
                    store.insert(key.clone(), (value.clone(), timestamp));
                    self.log(&format!("PUT {} {} {}", key, value, timestamp));

                    println!(
                        "[Node {}] (OWNER) PUT key={} value={}",
                        self.port, key, value
                    );
                }
            }
        }
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
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        println!("Usage: {} <port> [node1 node2 ...]", args[0]);
        return;
    }

    let port: u16 = args[1].parse().unwrap();

    let nodes: Vec<String> = if args.len() > 2 {
        args[2..].to_vec()
    } else {
        vec![]
    };

    let log_file = format!("data_{}.log", port);

    println!("[Node {}] Nodes: {:?}", port, nodes);

    let kv = KvStore::new(&log_file, nodes.clone(), port);
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

    let routes = put.or(get);

    println!("[Node {}] Server running", port);

    warp::serve(routes).run(([0, 0, 0, 0], port)).await;
}