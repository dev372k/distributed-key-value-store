use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Write, BufRead, BufReader};
use std::sync::{Arc, Mutex};
use warp::Filter;
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use tokio::time::{sleep, Duration};

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

    // Load data from log (persistent storage)
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

    // Append operation to log
    fn log(&self, command: &str) {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_file)
            .unwrap();

        writeln!(file, "{}", command).unwrap();
    }

    // Build consistent hashing ring
    fn sorted_ring(&self) -> Vec<(u64, String)> {
        let mut ring: Vec<(u64, String)> = self
            .nodes
            .iter()
            .map(|n| (hash_str(n), n.clone()))
            .collect();

        ring.sort_by_key(|k| k.0);
        ring
    }

    // Find primary + 2 replicas
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

    // PUT with routing + replication
    async fn put(&self, key: String, value: String) {
        let nodes = self.find_nodes(&key);
        let primary = &nodes[0];

        // Forward if not primary
        if &self.node_address != primary {
            let url = format!("{}/put?key={}&value={}", primary, key, value);
            let _ = reqwest::get(&url).await;
            return;
        }

        // Timestamp for last-write-wins
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        {
            let mut store = self.store.lock().unwrap();
            store.insert(key.clone(), (value.clone(), ts));
            self.log(&format!("PUT {} {} {}", key, value, ts));
        }

        // Replicate to other nodes
        for replica in nodes.iter().skip(1) {
            let url = format!(
                "{}/replicate?key={}&value={}&ts={}",
                replica, key, value, ts
            );

            let _ = reqwest::get(&url).await;
        }
    }

    // GET (latest timestamp wins)
    async fn get(&self, key: String) -> Option<String> {
        let nodes = self.find_nodes(&key);

        // Create async requests (but don't await yet)
        let requests = nodes.iter().map(|node| {
            let url = format!("{}/internal_get?key={}", node, key);
            async move {
                match reqwest::get(&url).await {
                    Ok(resp) => match resp.text().await {
                        Ok(text) => Some(text),
                        Err(_) => None,
                    },
                    Err(_) => None,
                }
            }
        });

        // Run all requests in parallel
        let results = join_all(requests).await;

        // Pick latest value
        let mut best_value: Option<String> = None;
        let mut best_ts: u128 = 0;

        for result in results {
            if let Some(text) = result {
                let parts: Vec<&str> = text.split("|").collect();

                if parts.len() == 2 {
                    let value = parts[0];
                    let ts: u128 = parts[1].parse().unwrap_or(0);

                    if ts > best_ts {
                        best_ts = ts;
                        best_value = Some(value.to_string());
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

    // Clone BEFORE moving into closure
    let kv_for_filter = kv.clone();
    let kv_filter = warp::any().map(move || kv_for_filter.clone());

    // PUT endpoint
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

    // GET endpoint
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

    // Replication endpoint
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

    // Internal read endpoint (used by quorum GET)
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

    // Anti-entropy (periodic sync)
    let kv_background = kv.clone();
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(10)).await;

            for node in &kv_background.nodes {
                if node == &kv_background.node_address {
                    continue;
                }

                let url = format!("{}/dump", node);

                if let Ok(resp) = reqwest::get(&url).await {
                    if let Ok(text) = resp.text().await {
                        for line in text.lines() {
                            let parts: Vec<&str> = line.split("|").collect();
                            if parts.len() == 3 {
                                let k = parts[0];
                                let v = parts[1];
                                let ts: u128 = parts[2].parse().unwrap_or(0);

                                let mut local = kv_background.store.lock().unwrap();

                                match local.get(k) {
                                    Some((_, existing_ts)) if *existing_ts >= ts => {}
                                    _ => {
                                        local.insert(k.to_string(), (v.to_string(), ts));
                                        println!(
                                            "[{}] Anti-entropy fixed {}",
                                            kv_background.port, k
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    });

    // Dump endpoint (for anti-entropy)
    let dump = warp::path("dump")
        .and(kv_filter.clone())
        .map(|kv: KvStore| {
            let store = kv.store.lock().unwrap();
            let mut result = String::new();

            for (k, (v, ts)) in store.iter() {
                result.push_str(&format!("{}|{}|{}\n", k, v, ts));
            }

            result
        });

    let routes = put.or(get).or(replicate).or(internal_get).or(dump);

    println!("[Node {}] Server running", port);

    warp::serve(routes).run(([0, 0, 0, 0], port)).await;
}