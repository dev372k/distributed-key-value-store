import http from 'k6/http';
import { sleep, check } from 'k6';

export let options = {
  scenarios: {
    // 1. Normal traffic
    normal_load: {
      executor: 'constant-vus',
      vus: 10,
      duration: '30s',
    },

    // 2. Spike traffic (sudden surge)
    spike_load: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '10s', target: 50 },  // spike up
        { duration: '20s', target: 50 },  // hold
        { duration: '10s', target: 0 },   // drop
      ],
      startTime: '30s',
    },

    // 3. Read-heavy workload
    read_heavy: {
      executor: 'constant-vus',
      vus: 20,
      duration: '30s',
      startTime: '1m10s',
    },
  },
};

const raw = open('../public_ips.txt');

const nodes = raw
  .split('\n')
  .map(ip => ip.trim())
  .filter(ip => ip.length > 0)
  .map(ip => `http://${ip}:3030`);

// Retry logic (your Option 2)
function requestWithRetry(path) {
  for (let i = 0; i < nodes.length; i++) {
    let url = `${nodes[i]}${path}`;
    let res = http.get(url);

    if (res.status === 200) {
      return res;
    }
  }
  return null;
}

// Hot keys (simulate popular data)
const hotKeys = ['user', 'city', 'session', 'cache'];

function randomNodeKey() {
  return `key_${Math.floor(Math.random() * 1000)}`;
}

export default function () {
  let rand = Math.random();

  // 🔹 70% reads, 30% writes (realistic workload)
  if (rand < 0.7) {
    // READ
    let key = Math.random() < 0.5
      ? hotKeys[Math.floor(Math.random() * hotKeys.length)]
      : randomNodeKey();

    let res = requestWithRetry(`/get?key=${key}`);

    check(res, {
      'GET success': (r) => r && r.status === 200,
    });

  } else {
    // WRITE
    let key = Math.random() < 0.5
      ? hotKeys[Math.floor(Math.random() * hotKeys.length)]
      : randomNodeKey();

    let value = `val_${Math.floor(Math.random() * 10000)}`;

    let res = requestWithRetry(`/put?key=${key}&value=${value}`);

    check(res, {
      'PUT success': (r) => r && r.status === 200,
    });
  }

  sleep(0.5);
}