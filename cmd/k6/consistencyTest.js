import http from 'k6/http';
import {check, sleep} from 'k6';

const BASE_URL = 'http://127.0.0.0';

export let options = {
    setupTimeout: '10m',
    teardownTimeout: '10m',
    stages: [
        {duration: '3s', target: 5},  // Hold steady at 50 users for 30s
    ],
    thresholds: {
        'http_req_duration': ['p(95)<500'], // 95% of requests should complete within 500ms
    },
};

export default function () {
    for (let i = 0; i < 10; i++) {
        let nodeId = Math.floor(Math.random() * 3); // Randomly select a node ID between 0 and 3
        let key = 'key' + Math.floor(Math.random() * 10); // write to 5 keys , pick randomly on of the 10 keys to write to it
        let value = 'value' + Math.floor(Math.random() * 1000); // Generate a random value

        let setRes = http.get(`${BASE_URL}:808${nodeId}/set?key=${key}&value=${value}`, {
            tags: {name: 'SetRequest'},
        });

        check(setRes, {
            'status is 200': (r) => r.status === 200,
        });
    }
    sleep(Math.floor(Math.random() * 2));
}

export function teardown() {
    let results = {
        0: {},
        1: {},
        2: {},
    }

    // wait for the replication batch to be executed
    sleep(10);

    for (let i = 0; i  <10; i++) {
        for (let j = 0; j < 3; j++) {
            const nodeId = j;
            let key = 'key' + i;

            let getRes = http.get(`${BASE_URL}:808${nodeId}/get?key=${key}`, {
                tags: {name: 'GetRequest'},
            });

            check(getRes, {
                'status is 200': (r) => r.status === 200,
            });

            const regex = /Replica shard = (\d+), coordinator shard = (\d+), current addr = (.*?), Value = (.*?), error = (.*)/;
            const body = getRes.body
            const match = body.match(regex);
            const value = match[4];

            results[nodeId][key] = value;
        }
    }

    // const keysToCheck = ['key0', 'key1', 'key2', 'key3', 'key4'];
    const keysToCheck = ['key0', 'key1', 'key2', 'key3', 'key4', 'key5', 'key6', 'key7', 'key8', 'key9'];

    for (const key of keysToCheck) {
        const values = Object.values(results).map(obj => obj[key]);
        console.log(values);

        for (let i = 0; i < values.length; i++) {
            check(values, {
                'Values are consistent': (values) => i>0? values[i] === values[i - 1] : true
            });
        }
    }
}

