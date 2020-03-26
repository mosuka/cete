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

### macOS

```bash
$ make GOOS=darwin dist
```


## Starting Cete node

Starting cete is easy as follows:

```bash
$ ./bin/cete start --id=node1 --bind-addr=:7000 --grpc-addr=:9000 --http-addr=:8000 --data-dir=/tmp/cete/node1
```

You can get the node information with the following command:

```bash
$ ./bin/cete node | jq .
```

or the following URL:

```bash
$ curl -X GET http://localhost:8000/v1/node | jq .
```

The result of the above command is:

```json
{
  "node": {
    "bind_addr": ":7000",
    "metadata": {
      "grpc_addr": ":9000",
      "http_addr": ":8000"
    },
    "state": "Leader"
  }
}
```

### Putting a key-value

To put a key-value, execute the following command:

```bash
$ ./bin/cete set 1 value1
```

or, you can use the RESTful API as follows:

```bash
$ curl -X PUT 'http://127.0.0.1:8000/v1/data/1' --data-binary value1
$ curl -X PUT 'http://127.0.0.1:8000/v1/data/2' -H "Content-Type: image/jpeg" --data-binary @/path/to/photo.jpg
```

### Getting a key-value

To get a key-value, execute the following command:

```bash
$ ./bin/cete get 1
```

or, you can use the RESTful API as follows:

```bash
$ curl -X GET 'http://127.0.0.1:8000/v1/data/1'
```

You can see the result. The result of the above command is:

```text
value1
```

### Deleting a value by key via CLI

Deleting a value by key, execute the following command:

```bash
$ ./bin/cete delete 1
```

or, you can use the RESTful API as follows:

```bash
$ curl -X DELETE 'http://127.0.0.1:8000/v1/data/1'
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
$ ./bin/cete cluster | jq .
```

or, you can use the RESTful API as follows:

```bash
$ curl -X GET 'http://127.0.0.1:8000/v1/cluster' | jq .
```

You can see the result in JSON format. The result of the above command is:

```json
{
  "cluster": {
    "nodes": {
      "node1": {
        "bind_addr": ":7000",
        "metadata": {
          "grpc_addr": ":9000",
          "http_addr": ":8000"
        },
        "state": "Leader"
      },
      "node2": {
        "bind_addr": ":7001",
        "metadata": {
          "grpc_addr": ":9001",
          "http_addr": ":8001"
        },
        "state": "Follower"
      },
      "node3": {
        "bind_addr": ":7002",
        "metadata": {
          "grpc_addr": ":9002",
          "http_addr": ":8002"
        },
        "state": "Follower"
      }
    },
    "leader": "node1"
  }
}
```

Recommend 3 or more odd number of nodes in the cluster. In failure scenarios, data loss is inevitable, so avoid deploying single nodes.

The above example, the node joins to the cluster at startup, but you can also join the node that already started on standalone mode to the cluster later, as follows:

```bash
$ ./bin/cete join --grpc-addr=:9000 node2 127.0.0.1:9001
```

or, you can use the RESTful API as follows:

```bash
$ curl -X PUT 'http://127.0.0.1:8000/v1/cluster/node2' --data-binary '
{
  "bind_addr": ":7001",
  "metadata": {
    "grpc_addr": ":9001",
    "http_addr": ":8001"
  }
}
'
```

To remove a node from the cluster, execute the following command:

```bash
$ ./bin/cete leave --grpc-addr=:9000 node2
```

or, you can use the RESTful API as follows:

```bash
$ curl -X DELETE 'http://127.0.0.1:8000/v1/cluster/node2'
```

The following command indexes documents to any node in the cluster:

```bash
$ ./bin/cete set --grpc-addr=:9000 1 value1
```

So, you can get the document from the node specified by the above command as follows:

```bash
$ ./bin/cete get --grpc-addr=:9000 1
```

You can see the result. The result of the above command is:

```text
value1
```

You can also get the same document from other nodes in the cluster as follows:

```bash
$ ./bin/cete get --grpc-addr=:9001 1
$ ./bin/cete get --grpc-addr=:9002 1
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
      --id=node1 \
      --bind-addr=:7000 \
      --grpc-addr=:9000 \
      --http-addr=:8000 \
      --data-dir=/tmp/cete/node1
```

You can execute the command in docker container as follows:

```bash
$ docker exec -it cete-node1 cete node --grpc-addr=:9000
```

## Securing Cete

Cete supports HTTPS access, ensuring that all communication between clients and a cluster is encrypted.

### Generating a certificate and private key

One way to generate the necessary resources is via [openssl](https://www.openssl.org/). For example:

```bash
$ openssl req -x509 -nodes -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365
Generating a 4096 bit RSA private key
............................++
........++
writing new private key to 'key.pem'
-----
You are about to be asked to enter information that will be incorporated
into your certificate request.
What you are about to enter is what is called a Distinguished Name or a DN.
There are quite a few fields but you can leave some blank
For some fields there will be a default value,
If you enter '.', the field will be left blank.
-----
Country Name (2 letter code) []:JP
State or Province Name (full name) []:Tokyo
Locality Name (eg, city) []:Minato
Organization Name (eg, company) []:Cete Project
Organizational Unit Name (eg, section) []:Operations
Common Name (eg, fully qualified host name) []:cete.example.org
Email Address []:admin@example.org
```

### Secure cluster example

Starting a node with HTTPS enabled, node-to-node encryption, and with the above configuration file. It is assumed the HTTPS X.509 certificate and key are at the paths server.crt and key.pem respectively.

```bash
$ ./bin/cete start --id=node1 --bind-addr=:7000 --grpc-addr=:9000 --http-addr=:8000 --data-dir=/tmp/cete/node1 --peer-grpc-addr=:9000 --cert-file=./cert.pem --key-file=./key.pem --cert-hostname=cete.example.org
$ ./bin/cete start --id=node2 --bind-addr=:7001 --grpc-addr=:9001 --http-addr=:8001 --data-dir=/tmp/cete/node2 --peer-grpc-addr=:9000 --cert-file=./cert.pem --key-file=./key.pem --cert-hostname=cete.example.org
$ ./bin/cete start --id=node3 --bind-addr=:7002 --grpc-addr=:9002 --http-addr=:8002 --data-dir=/tmp/cete/node3 --peer-grpc-addr=:9000 --cert-file=./cert.pem --key-file=./key.pem --cert-hostname=cete.example.org
```

You can access the cluster by adding a flag, such as the following command:

```bash
./bin/cete cluster --grpc-addr=:9000 --cert-file=./cert.pem --cert-hostname=cete.example.org | jq .
```
