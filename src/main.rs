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
    node_address: String,
}

impl KvStore {
    fn new(
        log_file: &str,
        nodes: Vec<String>,
        port: u16,
        node_address: String,
    ) -> Self {
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

    // ✅ FIXED GET (quorum-style read: latest timestamp wins)
    async fn get(&self, key: String) -> Option<String> {
        let nodes = self.find_nodes(&key);

        let mut best_value: Option<String> = None;
        let mut best_ts: u128 = 0;

        for node in nodes {
            let url = format!("{}/internal_get?key={}", node, key);

            if let Ok(resp) = reqwest::get(&url).await {
                if let Ok(text) = resp.text().await {
                    if let Some((value, ts)) = text.split_once("|") {
                        let ts: u128 = ts.parse().unwrap_or(0);

                        if ts > best_ts {
                            best_ts = ts;
                            best_value = Some(value.to_string());
                        }
                    }
                }
            }
        }

        best_value
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 4 {
        println!("Usage:");
        println!("{} <port> <self_node> <node1> <node2> ...", args[0]);
        return;
    }

    let port: u16 = args[1].parse().unwrap();
    let node_address = args[2].clone();
    let nodes: Vec<String> = args[3..].to_vec();

    let log_file = format!("data_{}.log", port);

    println!("[Node {}] Self: {}", port, node_address);
    println!("[Node {}] Cluster: {:?}", port, nodes);

    let kv = KvStore::new(&log_file, nodes.clone(), port, node_address);
    let kv_filter = warp::any().map(move || kv.clone());

    // PUT
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

    // ✅ UPDATED GET (async)
    let get = warp::path("get")
        .and(warp::query::<HashMap<String, String>>())
        .and(kv_filter.clone())
        .and_then(|params: HashMap<String, String>, kv: KvStore| async move {
            if let Some(k) = params.get("key") {
                match kv.get(k.clone()).await {
                    Some(val) => Ok::<_, warp::Rejection>(val),
                    None => Ok("Not found".to_string()),
                }
            } else {
                Ok("error".to_string())
            }
        });

    // REPLICATION
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

    // ✅ INTERNAL READ (for replicas)
    let internal_get = warp::path("internal_get")
        .and(warp::query::<HashMap<String, String>>())
        .and(kv_filter.clone())
        .map(|params: HashMap<String, String>, kv: KvStore| {
            if let Some(k) = params.get("key") {
                let store = kv.store.lock().unwrap();
                if let Some((v, ts)) = store.get(k) {
                    return format!("{}|{}", v, ts);
                }
            }
            "none|0".to_string()
        });

    let routes = put.or(get).or(replicate).or(internal_get);

    println!("[Node {}] Server running", port);

    warp::serve(routes).run(([0, 0, 0, 0], port)).await;
}