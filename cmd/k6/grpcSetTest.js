import grpc from 'k6/net/grpc';
import { Counter } from 'k6/metrics';
import { check } from 'k6';
import { randomItem } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js'

const nodes = ['localhost:8080', 'localhost:8081', 'localhost:8082', 'localhost:8083']
const client = new grpc.Client();

const service = client.load([ '../../coordinator/grpc/proto'], 'commands.proto');

const grpcErrors = new Counter('grpc_errors');

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
    let key = 'key' + Math.floor(Math.random() * 1000); // Generate a random key
    let value = 'value' + Math.floor(Math.random() * 1000);

    client.connect(randomItem(nodes), {
        plaintext: true,
    });
    const data = { key: key, value: value, coordinator: true };

    let getRes = client.invoke('commands.NodeService/Set', data, {  tags: { name: 'SetRequest'} } );
    check(getRes, {
        'status is OK': (r) => r && r.status === grpc.StatusOK,
    }) || grpcErrors.add(1);

    client.close();
}
