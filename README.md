<!--
 Copyright (c) 2020 Minoru Osuka

 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Cete

Cete is a distributed key value store server written in [Go](https://golang.org) built on top of [BadgerDB](https://blog.dgraph.io/post/badger/).  
It provides functions through [gRPC](http://www.grpc.io) ([HTTP/2](https://en.wikipedia.org/wiki/HTTP/2) + [Protocol Buffers](https://developers.google.com/protocol-buffers/)) or traditional [RESTful](https://en.wikipedia.org/wiki/Representational_state_transfer) API ([HTTP/1.1](https://en.wikipedia.org/wiki/Hypertext_Transfer_Protocol) + [JSON](http://www.json.org)).  
Cete implements [Raft consensus algorithm](https://raft.github.io/) by [hashicorp/raft](https://github.com/hashicorp/raft). It achieve consensus across all the instances of the nodes, ensuring that every change made to the system is made to a quorum of nodes, or none at all.  
Cete makes it easy bringing up a cluster of BadgerDB (a cete of badgers) .




## Features

- Easy deployment
- Bringing up cluster
- Database replication
- An easy-to-use HTTP API
- CLI is also available
- Docker container image is available


## Building Cete

When you satisfied dependencies, let's build Cete for Linux as following:

```bash
$ mkdir -p ${GOPATH}/src/github.com/mosuka
$ cd ${GOPATH}/src/github.com/mosuka
$ git clone https://github.com/mosuka/cete.git
$ cd cete
$ make build
```

If you want to build for other platform, set `GOOS`, `GOARCH` environment variables. For example, build for macOS like following:

```bash
$ make GOOS=darwin build
```


### Binaries

You can see the binary file when build successful like so:

```bash
$ ls ./bin
cete
```


## Testing Cete

If you want to test your changes, run command like following:

```bash
$ make test
```


## Packaging Cete

###  Linux

```bash
$ make GOOS=linux dist
```


#### macOS

```bash
$ make GOOS=darwin dist
```



## Starting Cete node

Starting cete is easy as follows:

```bash
$ ./bin/cete start --id=node1 --bind-addr=:7000 --grpc-addr=:9000 --http-addr=:8000 --data-dir=/tmp/cete/node1
```

You can get node info as follows: 

```bash
$ ./bin/cete node --grpc-addr=:9000
```

The result of the above command is:

```json
{
  "node": {
    "bind_addr": ":7000",
    "grpc_addr": ":9000",
    "http_addr": ":8000",
    "state": "Leader"
  }
}
```

### Setting a value by key via CLI

Setting a value by key, execute the following command:

```bash
$ ./bin/cete set --grpc-addr=:9000 --key=key1 value1
```


### Getting a value by key via CLI

Getting a value by key, execute the following command:

```bash
$ ./bin/cete get --grpc-addr=:9000 --key=key1
```

You can see the result. The result of the above command is:

```text
value1
```


### Deleting a value by key via CLI

Deleting a value by key, execute the following command:

```bash
$ ./bin/cete delete --grpc-addr=:9000 --key=key1
```


## Using HTTP REST API

Also you can do above commands via HTTP REST API that listened port 8000.


### Indexing a value by key via HTTP REST API

Indexing a value by key via HTTP is as following:

```bash
$ curl -s -X PUT 'http://127.0.0.1:8000/store/key1' -d value1
```


### Getting a value by key via HTTP REST API

Getting a value by key via HTTP is as following:

```bash
$ curl -s -X GET 'http://127.0.0.1:8000/store/key1'
```


### Deleting a value by key via HTTP REST API

Deleting a value by key via HTTP is as following:

```bash
$ curl -X DELETE 'http://127.0.0.1:8000/store/key1'
```


## Bringing up a cluster

Cete is easy to bring up the cluster. Cete node is already running, but that is not fault tolerant. If you need to increase the fault tolerance, bring up 2 more data nodes like so:

```bash
$ ./bin/cete start --id=node2 --bind-addr=:7001 --grpc-addr=:9001 --http-addr=:8001 --data-dir=/tmp/cete/node2 --peer-grpc-addr=:9000
$ ./bin/cete start --id=node3 --bind-addr=:7002 --grpc-addr=:9002 --http-addr=:8002 --data-dir=/tmp/cete/node3 --peer-grpc-addr=:9000
```

_Above example shows each Cete node running on the same host, so each node must listen on different ports. This would not be necessary if each node ran on a different host._

This instructs each new node to join an existing node, each node recognizes the joining clusters when started.
So you have a 3-node cluster. That way you can tolerate the failure of 1 node. You can check the cluster with the following command:

```bash
$ ./bin/cete cluster --grpc-addr=:9000
```

You can see the result in JSON format. The result of the above command is:

```json
{
  "nodes": {
    "node1": {
      "bind_addr": ":7000",
      "grpc_addr": ":9000",
      "http_addr": ":8000",
      "state": "Leader"
    },
    "node2": {
      "bind_addr": ":7001",
      "grpc_addr": ":9001",
      "http_addr": ":8001",
      "state": "Follower"
    },
    "node3": {
      "bind_addr": ":7002",
      "grpc_addr": ":9002",
      "http_addr": ":8002",
      "state": "Follower"
    }
  }
}
```

Recommend 3 or more odd number of nodes in the cluster. In failure scenarios, data loss is inevitable, so avoid deploying single nodes.

The following command indexes documents to any node in the cluster:

```bash
$ ./bin/cete set --grpc-addr=:9000 --key=key1 value1
```

So, you can get the document from the node specified by the above command as follows:

```bash
$ ./bin/cete get --grpc-addr=:9000 --key=key1
```

You can see the result. The result of the above command is:

```text
value1
```

You can also get the same document from other nodes in the cluster as follows:

```bash
$ ./bin/cete get --grpc-addr=:9001 --key=key1
$ ./bin/cete get --grpc-addr=:9002 --key=key1
```

You can see the result. The result of the above command is:

```text
value1
```


## Cete on Docker

### Building Cete Docker container image on localhost

You can build the Docker container image like so:

```bash
$ make docker-build
```

### Pulling Cete Docker container image from docker.io

You can also use the Docker container image already registered in docker.io like so:

```bash
$ docker pull mosuka/cete:latest
```

See https://hub.docker.com/r/mosuka/cete/tags/


### Pulling Cete Docker container image from docker.io

You can also use the Docker container image already registered in docker.io like so:

```bash
$ docker pull mosuka/cete:latest
```


### Running Cete node on Docker

Running a Cete data node on Docker. Start Cete node like so:

```bash
$ docker run --rm --name cete-node1 \
    -p 7000:7000 \
    -p 8000:8000 \
    -p 9000:9000 \
    mosuka/cete:latest cete start \
      --node-id=node1 \
      --bind-addr=:7000 \
      --grpc-addr=:9000 \
      --http-addr=:8000 \
      --data-dir=/tmp/cete/node1
```

You can execute the command in docker container as follows:

```bash
$ docker exec -it cete-node1 cete node --grpc-addr=:9000
```
