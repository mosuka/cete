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

## Configure Cete

| CLI Flag | Environment variable | Configuration File | Description |
| --- | --- | --- | --- |
| --config-file | - | - | config file. if omitted, cete.yaml in /etc and home directory will be searched |
| --id | CETE_ID | id | node ID |
| --raft-address | CETE_RAFT_ADDRESS | raft_address | Raft server listen address |
| --grpc-address | CETE_GRPC_ADDRESS | grpc_address | gRPC server listen address |
| --http-address | CETE_HTTP_ADDRESS | http_address | HTTP server listen address |
| --data-directory | CETE_DATA_DIRECTORY | data_directory | data directory which store the key-value store data and Raft logs |
| --peer-grpc-address | CETE_PEER_GRPC_ADDRESS | peer_grpc_address | listen address of the existing gRPC server in the joining cluster |
| --certificate-file | CETE_CERTIFICATE_FILE | certificate_file | path to the client server TLS certificate file |
| --key-file | CETE_KEY_FILE | key_file | path to the client server TLS key file |
| --common-name | CETE_COMMON_NAME | common_name | certificate common name |
| --log-level | CETE_LOG_LEVEL | log_level | log level |
| --log-file | CETE_LOG_FILE | log_file | log file |
| --log-max-size | CETE_LOG_MAX_SIZE | log_max_size | max size of a log file in megabytes |
| --log-max-backups | CETE_LOG_MAX_BACKUPS | log_max_backups | max backup count of log files |
| --log-max-age | CETE_LOG_MAX_AGE | log_max_age | max age of a log file in days |
| --log-compress | CETE_LOG_COMPRESS | log_compress | compress a log file |


## Starting Cete node

Starting cete is easy as follows:

```bash
$ ./bin/cete start --id=node1 --raft-address=:7000 --grpc-address=:9000 --http-address=:8000 --data-directory=/tmp/cete/node1
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

## Putting a key-value

To put a key-value, execute the following command:

```bash
$ ./bin/cete set 1 value1
```

or, you can use the RESTful API as follows:

```bash
$ curl -X PUT 'http://127.0.0.1:8000/v1/data/1' --data-binary value1
$ curl -X PUT 'http://127.0.0.1:8000/v1/data/2' -H "Content-Type: image/jpeg" --data-binary @/path/to/photo.jpg
```

## Getting a key-value

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

## Deleting a key-value

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
$ ./bin/cete start --id=node2 --raft-address=:7001 --grpc-address=:9001 --http-address=:8001 --data-directory=/tmp/cete/node2 --peer-grpc-address=:9000
$ ./bin/cete start --id=node3 --raft-address=:7002 --grpc-address=:9002 --http-address=:8002 --data-directory=/tmp/cete/node3 --peer-grpc-address=:9000
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
$ openssl req -x509 -nodes -newkey rsa:4096 -keyout ./etc/cete-key.pem -out ./etc/cete-cert.pem -days 365 -subj '/CN=localhost'
Generating a 4096 bit RSA private key
............................++
........++
writing new private key to 'key.pem'
```

### Secure cluster example

Starting a node with HTTPS enabled, node-to-node encryption, and with the above configuration file. It is assumed the HTTPS X.509 certificate and key are at the paths server.crt and key.pem respectively.

```bash
$ ./bin/cete start --id=node1 --bind-addr=:7000 --grpc-addr=:9000 --http-addr=:8000 --data-dir=/tmp/cete/node1 --peer-grpc-addr=:9000 --cert-file=./etc/cert.pem --key-file=./etc/key.pem --cert-hostname=localhost
$ ./bin/cete start --id=node2 --bind-addr=:7001 --grpc-addr=:9001 --http-addr=:8001 --data-dir=/tmp/cete/node2 --peer-grpc-addr=:9000 --cert-file=./etc/cert.pem --key-file=./etc/key.pem --cert-hostname=localhost
$ ./bin/cete start --id=node3 --bind-addr=:7002 --grpc-addr=:9002 --http-addr=:8002 --data-dir=/tmp/cete/node3 --peer-grpc-addr=:9000 --cert-file=./etc/cert.pem --key-file=./etc/key.pem --cert-hostname=localhost
```

You can access the cluster by adding a flag, such as the following command:

```bash
$ ./bin/cete cluster --grpc-addr=:9000 --cert-file=./cert.pem --cert-hostname=localhost | jq .
```

or

```bash
$ curl -X GET https://localhost:8000/v1/cluster --cacert ./cert.pem | jq .
```
