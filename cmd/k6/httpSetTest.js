import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = 'http://127.0.0.0';

export let options = {
    stages: [
        { duration: '1m', target: 50 },  // Ramp up to 50 users over 1 minute
        { duration: '1m', target: 50 },  // Hold steady at 50 users for 1 minutes
        { duration: '1m', target: 0 },    // Ramp down to 0 users over 1 minute
    ],
    thresholds: {
        'http_req_duration': ['p(95)<500'], // 95% of requests should complete within 500ms
    },
};

export default function () {
    let nodeId = Math.floor(Math.random() * 3); // Randomly select a node ID between 0 and 3
    let key = 'key' + Math.floor(Math.random() * 1000); // Generate a random key
    let value = 'value' + Math.floor(Math.random() * 1000); // Generate a random value

    let setRes = http.get(`${BASE_URL}:808${nodeId}/set?key=${key}&value=${value}`, {
        tags: { name: 'SetRequest'},
    });
    check(setRes, {
        'status is 200': (r) => r.status === 200,
    });
}
