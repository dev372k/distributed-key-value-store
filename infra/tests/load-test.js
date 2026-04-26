import http from 'k6/http';
import { sleep, check } from 'k6';

const raw = open('../public_ips.txt');

const nodes = raw
  .split('\n')
  .map(ip => ip.trim())
  .filter(ip => ip.length > 0)
  .map(ip => `http://${ip}:3030`);

export let options = {
  vus: 10,
  duration: '30s',
};

// // Stress test
// export let options = {
//   vus: 50,
//   duration: '1m',
// };

function randomNode() {
  return nodes[Math.floor(Math.random() * nodes.length)];
}

export default function () {
  let key = `key_${Math.floor(Math.random() * 1000)}`;
  let value = `val_${Math.floor(Math.random() * 1000)}`;

  // PUT
  let putRes = http.get(`${randomNode()}/put?key=${key}&value=${value}`);
  check(putRes, {
    'PUT success': (r) => r.status === 200,
  });

  // GET
  let getRes = http.get(`${randomNode()}/get?key=${key}`);
  check(getRes, {
    'GET success': (r) => r.status === 200,
  });

  sleep(1);
}