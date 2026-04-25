use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Write, BufRead, BufReader};
use std::sync::{Arc, Mutex};
use warp::Filter;
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

fn hash_str(input: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    input.hash(&mut hasher);
    hasher.finish()
}

#[derive(Clone)]
struct KvStore {
    store: Arc<Mutex<HashMap<String, (String, u128)>>>,
    log_file: String,
    nodes: Vec<String>,
    port: u16,
    node_address: String, // ✅ FIXED
}

impl KvStore {
    fn new(log_file: &str, nodes: Vec<String>, port: u16) -> Self {
        let node_address = nodes
            .iter()
            .find(|n| n.ends_with(&format!(":{}", port)))
            .expect("Node address not found")
            .clone();

        let kv = KvStore {
            store: Arc::new(Mutex::new(HashMap::new())),
            log_file: log_file.to_string(),
            nodes,
            port,
            node_address,
        };

        kv.load();
        kv
    }

    fn load(&self) {
        if let Ok(file) = OpenOptions::new().read(true).open(&self.log_file) {
            let reader = BufReader::new(file);
            let mut store = self.store.lock().unwrap();

            for line in reader.lines().flatten() {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() == 4 && parts[0] == "PUT" {
                    let ts: u128 = parts[3].parse().unwrap_or(0);
                    store.insert(parts[1].to_string(), (parts[2].to_string(), ts));
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

    fn sorted_ring(&self) -> Vec<(u64, String)> {
        let mut ring: Vec<(u64, String)> = self
            .nodes
            .iter()
            .map(|n| (hash_str(n), n.clone()))
            .collect();

        ring.sort_by_key(|k| k.0);
        ring
    }

    fn find_nodes(&self, key: &str) -> Vec<String> {
        let ring = self.sorted_ring();
        let key_hash = hash_str(key);

        for (i, (node_hash, _)) in ring.iter().enumerate() {
            if *node_hash >= key_hash {
                return (0..3)
                    .map(|j| ring[(i + j) % ring.len()].1.clone())
                    .collect();
            }
        }

        (0..3).map(|i| ring[i].1.clone()).collect()
    }

    async fn put(&self, key: String, value: String) {
        let nodes = self.find_nodes(&key);
        let primary = &nodes[0];

        if &self.node_address != primary {
            println!("[{}] Forwarding {} → {}", self.port, key, primary);

            let url = format!("{}/put?key={}&value={}", primary, key, value);
            let _ = reqwest::get(&url).await;
            return;
        }

        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        {
            let mut store = self.store.lock().unwrap();
            store.insert(key.clone(), (value.clone(), ts));
            self.log(&format!("PUT {} {} {}", key, value, ts));
        }

        println!("[{}] Stored {} locally", self.port, key);

        for replica in nodes.iter().skip(1) {
            let url = format!(
                "{}/replicate?key={}&value={}&ts={}",
                replica, key, value, ts
            );

            let _ = reqwest::get(&url).await;
        }
    }

    fn get(&self, key: String) -> Option<String> {
        let store = self.store.lock().unwrap();
        store.get(&key).map(|(v, _)| v.clone())
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    let port: u16 = args[1].parse().unwrap();
    let nodes: Vec<String> = args[2..].to_vec();

    let log_file = format!("data_{}.log", port);

    println!("[Node {}] Nodes: {:?}", port, nodes);

    let kv = KvStore::new(&log_file, nodes.clone(), port);
    let kv_filter = warp::any().map(move || kv.clone());

    let put = warp::path("put")
        .and(warp::query::<HashMap<String, String>>())
        .and(kv_filter.clone())
        .and_then(|params: HashMap<String, String>, kv: KvStore| async move {
            if let (Some(k), Some(v)) = (params.get("key"), params.get("value")) {
                kv.put(k.clone(), v.clone()).await;
                Ok::<_, warp::Rejection>("OK".to_string())
            } else {
                Ok("error".to_string())
            }
        });

    let get = warp::path("get")
        .and(warp::query::<HashMap<String, String>>())
        .and(kv_filter.clone())
        .map(|params: HashMap<String, String>, kv: KvStore| {
            if let Some(k) = params.get("key") {
                kv.get(k.clone()).unwrap_or("Not found".to_string())
            } else {
                "error".to_string()
            }
        });

    let replicate = warp::path("replicate")
        .and(warp::query::<HashMap<String, String>>())
        .and(kv_filter.clone())
        .map(|params: HashMap<String, String>, kv: KvStore| {
            if let (Some(k), Some(v), Some(ts)) =
                (params.get("key"), params.get("value"), params.get("ts"))
            {
                let ts: u128 = ts.parse().unwrap();
                let mut store = kv.store.lock().unwrap();
                store.insert(k.clone(), (v.clone(), ts));
                kv.log(&format!("PUT {} {} {}", k, v, ts));
            }
            "OK"
        });

    let routes = put.or(get).or(replicate);

    println!("[Node {}] Server running", port);

    warp::serve(routes).run(([0, 0, 0, 0], port)).await;
}