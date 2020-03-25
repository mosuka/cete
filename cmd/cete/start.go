// Copyright (c) 2020 Minoru Osuka
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mosuka/cete/client"
	"github.com/mosuka/cete/errors"
	"github.com/mosuka/cete/log"
	"github.com/mosuka/cete/protobuf"
	"github.com/mosuka/cete/server"
	"github.com/urfave/cli"
	"go.uber.org/zap"
)

func execStart(ctx *cli.Context) error {
	nodeId := ctx.String("id")
	bindAddr := ctx.String("bind-addr")
	grpcAddr := ctx.String("grpc-addr")
	httpAddr := ctx.String("http-addr")
	dataDir := ctx.String("data-dir")
	peerGrpcAddr := ctx.String("peer-grpc-addr")

	certFile := ctx.String("cert-file")
	keyFile := ctx.String("key-file")
	certHostname := ctx.String("cert-hostname")

	logger := log.NewLogger(
		ctx.String("log-level"),
		ctx.String("log-file"),
		ctx.Int("log-max-size"),
		ctx.Int("log-max-backups"),
		ctx.Int("log-max-age"),
		ctx.Bool("log-compress"),
	)

	bootstrap := peerGrpcAddr == "" || peerGrpcAddr == grpcAddr

	raftServer, err := server.NewRaftServer(nodeId, bindAddr, dataDir, bootstrap, logger)
	if err != nil {
		logger.Error("failed to create Raft server",
			zap.String("id", nodeId),
			zap.String("bind_addr", bindAddr),
			zap.String("data_dir", dataDir),
			zap.Bool("bootstrap", bootstrap),
			zap.String("err", err.Error()),
		)
		return err
	}

	// TODO: TLS support for gRPC will be done later.
	//grpcServer, err := server.NewGRPCServer(grpcAddr, raftServer, certFile, keyFile, certHostname, logger)
	grpcServer, err := server.NewGRPCServer(grpcAddr, raftServer, "", "", "", logger)
	if err != nil {
		logger.Error("failed to create gRPC server", zap.String("grpc_addr", grpcAddr), zap.Error(err))
		return err
	}

	grpcGateway, err := server.NewGRPCGateway(httpAddr, grpcAddr, certFile, keyFile, certHostname, logger)
	if err != nil {
		logger.Error("failed to create gRPC gateway", zap.String("http_addr", httpAddr), zap.String("grpc_addr", grpcAddr), zap.Error(err))
		return err
	}

	quitCh := make(chan os.Signal, 1)
	signal.Notify(quitCh, os.Kill, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	if err := raftServer.Start(); err != nil {
		logger.Error("failed to start Raft server", zap.Error(err))
		return err
	}

	if err := grpcServer.Start(); err != nil {
		logger.Error("failed to start gRPC server", zap.Error(err))
		return err
	}

	if err := grpcGateway.Start(); err != nil {
		logger.Error("failed to start gRPC gateway", zap.Error(err))
		return err
	}

	// wait for detect leader if it's bootstrap
	if bootstrap {
		timeout := 60 * time.Second
		if err := raftServer.WaitForDetectLeader(timeout); err != nil {
			if err == errors.ErrTimeout {
				logger.Error("leader detection timed out", zap.Duration("timeout", timeout), zap.Error(err))
			} else {
				logger.Error("failed to detect leader", zap.Error(err))
			}
			return err
		}
	}

	// create gRPC client for joining node
	var joinAddr string
	if bootstrap {
		joinAddr = grpcAddr
	} else {
		joinAddr = peerGrpcAddr
	}

	// TODO: TLS support for gRPC will be done later.
	//c, err := client.NewGRPCClientWithContextTLS(joinAddr, context.Background(), certFile, certHostname)
	c, err := client.NewGRPCClientWithContext(joinAddr, context.Background())
	if err != nil {
		logger.Error("failed to create gRPC client", zap.String("addr", joinAddr), zap.Error(err))
		return err
	}
	defer func() {
		_ = c.Close()
	}()

	// join this node to the existing cluster
	joinRequest := &protobuf.JoinRequest{
		Id: nodeId,
		Node: &protobuf.Node{
			BindAddr: bindAddr,
			Metadata: &protobuf.Metadata{
				GrpcAddr: grpcAddr,
				HttpAddr: httpAddr,
			},
		},
	}
	if err = c.Join(joinRequest); err != nil {
		logger.Error("failed to join node to the cluster", zap.Any("req", joinRequest), zap.Error(err))
		return err
	}

	// wait for receiving signal
	<-quitCh

	if err := grpcGateway.Stop(); err != nil {
		logger.Error("failed to stop gRPC gateway", zap.Error(err))
	}

	if err := grpcServer.Stop(); err != nil {
		logger.Error("failed to stop gRPC server", zap.Error(err))
	}

	if err := raftServer.Stop(); err != nil {
		logger.Error("failed to stop Raft server", zap.Error(err))
	}

	return nil
}
