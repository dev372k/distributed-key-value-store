use axum::{
    extract::Query,
    routing::{get, post},
    Json, Router,
};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha1::{Digest, Sha1};
use std::{
    collections::HashMap,
    env,
    net::SocketAddr,
    sync::Arc,
};
use tokio::sync::RwLock;

#[derive(Clone, Serialize, Deserialize)]
struct ValueEntry {
    value: String,
    ts: u128,
}

#[derive(Clone)]
struct AppState {
    store: Arc<RwLock<HashMap<String, ValueEntry>>>,
    ring: Arc<RwLock<Vec<(u64, String)>>>,
    nodes: Vec<String>,
    my_ip: String,
}

static REPLICATION_FACTOR: usize = 3;

#[derive(Deserialize)]
struct PutParams {
    key: String,
    value: String,
}

#[derive(Deserialize)]
struct GetParams {
    key: String,
}

fn hash_key(key: &str) -> u64 {
    let mut hasher = Sha1::new();
    hasher.update(key.as_bytes());

    let result = hasher.finalize();

    let bytes: [u8; 8] = result[..8].try_into().unwrap();

    u64::from_be_bytes(bytes)
}

async fn build_ring(nodes: Vec<String>) -> Vec<(u64, String)> {
    let mut ring = vec![];

    for node in nodes {
        ring.push((hash_key(&node), node));
    }

    ring.sort_by_key(|x| x.0);

    ring
}

async fn get_replicas(
    state: &AppState,
    key: &str,
) -> Vec<String> {
    let ring = state.ring.read().await;

    if ring.is_empty() {
        return vec![];
    }

    let h = hash_key(key);

    let mut idx = 0;

    for (i, (node_hash, _)) in ring.iter().enumerate() {
        if *node_hash >= h {
            idx = i;
            break;
        }
    }

    let mut replicas = vec![];

    for i in 0..REPLICATION_FACTOR {
        let node = ring[(idx + i) % ring.len()].1.clone();
        replicas.push(node);
    }

    replicas
}

async fn put(
    Query(params): Query<PutParams>,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Json<serde_json::Value> {

    let replicas = get_replicas(&state, &params.key).await;

    if replicas.is_empty() {
        return Json(json!({
            "error": "no replicas"
        }));
    }

    let primary = replicas[0].clone();

    // forward if not primary
    if state.my_ip != primary {

        let url = format!(
            "http://{}:3030/put?key={}&value={}",
            primary,
            params.key,
            params.value
        );

        match reqwest::get(url).await {
            Ok(resp) => {
                let body: serde_json::Value =
                    resp.json().await.unwrap();

                return Json(body);
            }

            Err(_) => {
                return Json(json!({
                    "error": "primary unreachable"
                }));
            }
        }
    }

    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    {
        let mut store = state.store.write().await;

        store.insert(
            params.key.clone(),
            ValueEntry {
                value: params.value.clone(),
                ts,
            },
        );
    }

    // async replication
    for replica in replicas.iter().skip(1) {

        let replica = replica.clone();

        let key = params.key.clone();
        let value = params.value.clone();

        tokio::spawn(async move {

            let client = reqwest::Client::new();

            let _ = client
                .post(format!(
                    "http://{}:3030/internal_put",
                    replica
                ))
                .json(&json!({
                    "key": key,
                    "value": value,
                    "ts": ts
                }))
                .send()
                .await;
        });
    }

    Json(json!({
        "status": "ok",
        "primary": primary,
        "replicas": replicas
    }))
}

async fn get_key(
    Query(params): Query<GetParams>,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Json<serde_json::Value> {

    let replicas = get_replicas(&state, &params.key).await;

    let mut latest: Option<ValueEntry> = None;

    // local-first optimization
    {
        let store = state.store.read().await;

        if let Some(v) = store.get(&params.key) {
            latest = Some(v.clone());
        }
    }

    // parallel replica reads
    let mut tasks = vec![];

    for node in replicas {

        let key = params.key.clone();

        tasks.push(tokio::spawn(async move {

            let url = format!(
                "http://{}:3030/local_get?key={}",
                node,
                key
            );

            match reqwest::get(url).await {

                Ok(resp) => {
                    resp.json::<ValueEntry>().await.ok()
                }

                Err(_) => None,
            }
        }));
    }

    for task in tasks {

        if let Ok(Some(v)) = task.await {

            if latest.is_none()
                || v.ts > latest.as_ref().unwrap().ts
            {
                latest = Some(v);
            }
        }
    }

    match latest {
        Some(v) => Json(json!({
            "value": v.value,
            "ts": v.ts
        })),

        None => Json(json!({
            "error": "not found"
        })),
    }
}

async fn local_get(
    Query(params): Query<GetParams>,
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Json<serde_json::Value> {

    let store = state.store.read().await;

    match store.get(&params.key) {

        Some(v) => Json(json!(v)),

        None => Json(json!({
            "error": "not found"
        })),
    }
}

#[derive(Deserialize)]
struct InternalPut {
    key: String,
    value: String,
    ts: u128,
}

async fn internal_put(
    axum::extract::State(state): axum::extract::State<AppState>,
    Json(data): Json<InternalPut>,
) -> Json<serde_json::Value> {

    let mut store = state.store.write().await;

    let update = match store.get(&data.key) {
        Some(v) => data.ts > v.ts,
        None => true,
    };

    if update {

        store.insert(
            data.key.clone(),
            ValueEntry {
                value: data.value.clone(),
                ts: data.ts,
            },
        );
    }

    Json(json!({
        "status": "replicated"
    }))
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({
        "status": "ok"
    }))
}

async fn dump(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Json<HashMap<String, ValueEntry>> {

    let store = state.store.read().await;

    Json(store.clone())
}

#[tokio::main]
async fn main() {

    let node_list =
        env::var("NODE_LIST").unwrap_or_default();

    let my_ip =
        env::var("MY_IP").unwrap_or("node1".to_string());

    let nodes: Vec<String> = node_list
        .split(',')
        .map(|s| s.to_string())
        .collect();

    let ring = build_ring(nodes.clone()).await;

    let state = AppState {
        store: Arc::new(RwLock::new(HashMap::new())),
        ring: Arc::new(RwLock::new(ring)),
        nodes,
        my_ip,
    };

    println!("Starting node: {}", state.my_ip);

    let app = Router::new()
        .route("/put", get(put))
        .route("/get", get(get_key))
        .route("/local_get", get(local_get))
        .route("/internal_put", post(internal_put))
        .route("/dump", get(dump))
        .route("/health", get(health))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3030));

    println!("Listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap();

    axum::serve(listener, app)
        .await
        .unwrap();
}