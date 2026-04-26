use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Write, BufRead, BufReader};
use std::sync::{Arc, Mutex};
use warp::Filter;
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use futures::future::join_all;
use tokio::time::Duration;
use reqwest::Client;

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
    node_address: String,
    client: Client,
}

impl KvStore {
    fn new(log_file: &str, nodes: Vec<String>, node_address: String) -> Self {
        let kv = KvStore {
            store: Arc::new(Mutex::new(HashMap::new())),
            log_file: log_file.to_string(),
            nodes,
            node_address,
            client: Client::builder()
                .timeout(Duration::from_secs(2)) // safer timeout
                .build()
                .unwrap(),
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
        if let Ok(mut file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_file)
        {
            let _ = writeln!(file, "{}", command);
        }
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
            let url = format!("{}/put?key={}&value={}", primary, key, value);
            let _ = self.client.get(&url).send().await;
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

        for replica in nodes.iter().skip(1) {
            let url = format!(
                "{}/replicate?key={}&value={}&ts={}",
                replica, key, value, ts
            );
            let _ = self.client.get(&url).send().await;
        }
    }

    async fn get(&self, key: String) -> Option<String> {
        let nodes = self.find_nodes(&key);

        let requests = nodes.iter().map(|node| {
            let client = self.client.clone();
            let url = format!("{}/internal_get?key={}", node, key);

            async move {
                match client.get(&url).send().await {
                    Ok(resp) => resp.text().await.ok(),
                    Err(_) => None,
                }
            }
        });

        let results = join_all(requests).await;

        let mut best_value: Option<String> = None;
        let mut best_ts: u128 = 0;

        for result in results {
            if let Some(text) = result {
                let parts: Vec<&str> = text.split('|').collect();

                if parts.len() == 2 {
                    let ts: u128 = parts[1].parse().unwrap_or(0);

                    if ts > best_ts {
                        best_ts = ts;
                        best_value = Some(parts[0].to_string());
                    }
                }
            }
        }

        best_value
    }
}

#[tokio::main]
async fn main() {
    println!("🚀 STARTING KV NODE");

    let args: Vec<String> = env::args().collect();
    println!("ARGS: {:?}", args);

    if args.len() < 4 {
        eprintln!("Usage: {} <port> <self_node> <nodes...>", args[0]);
        return;
    }

    // SAFE PARSE
    let port: u16 = match args[1].parse() {
        Ok(p) => p,
        Err(_) => {
            eprintln!("Invalid port");
            return;
        }
    };

    let node_address = args[2].clone();

    let mut nodes: Vec<String> = args[3..].to_vec();
    nodes.sort();
    nodes.dedup();

    println!("SELF: {}", node_address);
    println!("CLUSTER: {:?}", nodes);

    let log_file = format!("data_{}.log", port);
    let kv = KvStore::new(&log_file, nodes.clone(), node_address);

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

    let replicate = warp::path("replicate")
        .and(warp::query::<HashMap<String, String>>())
        .and(kv_filter.clone())
        .map(|params: HashMap<String, String>, kv: KvStore| {
            if let (Some(k), Some(v), Some(ts)) =
                (params.get("key"), params.get("value"), params.get("ts"))
            {
                let ts: u128 = ts.parse().unwrap_or(0);
                let mut store = kv.store.lock().unwrap();
                store.insert(k.clone(), (v.clone(), ts));
                kv.log(&format!("PUT {} {} {}", k, v, ts));
            }
            "OK"
        });

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

    println!("SERVER RUNNING on port {}", port);

    warp::serve(routes).run(([0, 0, 0, 0], port)).await;
}