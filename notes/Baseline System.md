#### Camera Feed
- this should currently send udp stream to server, they consist of random bytes.
#### Main Server
- Recieve this udp stream and send this to ML model.
#### Slow ML model

- This should be slow to print the stream recieved.

### Problems to solve

| #      | Problem we deliberately create                               | What it teaches                                                     |
| ------ | ------------------------------------------------------------ | ------------------------------------------------------------------- |
| **1**  | **Too many cameras for one server**                          | Distribution, horizontal scaling, partitioning cameras across nodes |
| **2**  | **Incoming stream is faster than processing**                | Queues, buffering, backpressure, bounded buffers                    |
| **3**  | **One processing worker isn't enough**                       | Worker pools, parallelism, scheduling, throughput vs latency        |
| **4**  | **A server/worker crashes**                                  | Failure detection, retries, reassignment, fault tolerance           |
| **5**  | **Multiple nodes need to coordinate**                        | Node membership, service discovery, distributed coordination        |
| **6**  | **Work needs to move between nodes efficiently**             | RPC/network protocols, connection management, serialization         |
| **7**  | **Events/data must survive process failure**                 | Persistence, durability, acknowledgements, idempotency              |
| **8**  | **Network failures cause loss/duplicates/out-of-order data** | Retries, ordering, deduplication, delivery semantics                |
| **9**  | **The system becomes impossible to understand/debug**        | Metrics, logs, distributed tracing, observability                   |
| **10** | **Managing many distributed processes becomes painful**      | Containers, orchestration, deployment, health checks                |
