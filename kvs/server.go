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

package kvs

import (
	"time"

	ceteerrors "github.com/mosuka/cete/errors"
	pbkvs "github.com/mosuka/cete/protobuf/kvs"
	"go.uber.org/zap"
)

type Server struct {
	nodeId   string
	bindAddr string
	grpcAddr string
	httpAddr string
	dataDir  string

	bootstrap    bool
	peerGrpcAddr string

	raftServer  *RaftServer
	grpcServer  *GRPCServer
	grpcGateway *GRPCGateway

	logger *zap.Logger
}

func NewServer(nodeId string, bindAddr string, grpcAddr string, httpAddr string, dataDir string, peerGrpcAddr string, logger *zap.Logger) (*Server, error) {
	bootstrap := peerGrpcAddr == "" || peerGrpcAddr == grpcAddr

	raftServer, err := NewRaftServer(nodeId, bindAddr, dataDir, bootstrap, logger)
	if err != nil {
		logger.Error("failed to create Raft server",
			zap.String("id", nodeId),
			zap.String("bind_addr", bindAddr),
			zap.String("data_dir", dataDir),
			zap.Bool("bootstrap", bootstrap),
			zap.String("err", err.Error()),
		)
		return nil, err
	}

	grpcServer, err := NewGRPCServer(grpcAddr, raftServer, logger)
	if err != nil {
		logger.Error("failed to create gRPC server", zap.String("grpc_addr", grpcAddr), zap.Error(err))
		return nil, err
	}

	grpcGateway, err := NewGRPCGateway(httpAddr, grpcAddr, logger)
	if err != nil {
		logger.Error("failed to create gRPC gateway", zap.String("http_addr", httpAddr), zap.String("grpc_addr", grpcAddr), zap.Error(err))
		return nil, err
	}

	server := &Server{
		nodeId:       nodeId,
		bindAddr:     bindAddr,
		grpcAddr:     grpcAddr,
		httpAddr:     httpAddr,
		dataDir:      dataDir,
		bootstrap:    bootstrap,
		peerGrpcAddr: peerGrpcAddr,
		raftServer:   raftServer,
		grpcServer:   grpcServer,
		grpcGateway:  grpcGateway,
		logger:       logger,
	}

	return server, nil
}

func (s *Server) Start() {
	if err := s.raftServer.Start(); err != nil {
		s.logger.Error("failed to start Raft server", zap.Error(err))
		return
	}

	if err := s.grpcServer.Start(); err != nil {
		s.logger.Error("failed to start gRPC server", zap.Error(err))
		return
	}

	if err := s.grpcGateway.Start(); err != nil {
		s.logger.Error("failed to start gRPC gateway", zap.Error(err))
		return
	}

	// wait for detect leader if it's bootstrap
	if s.bootstrap {
		timeout := 60 * time.Second
		if err := s.raftServer.WaitForDetectLeader(timeout); err != nil {
			if err == ceteerrors.ErrTimeout {
				s.logger.Error("leader detection timed out", zap.Duration("timeout", timeout), zap.Error(err))
			} else {
				s.logger.Error("failed to detect leader", zap.Error(err))
			}
			return
		}
	}

	// create gRPC client for joining node
	var joinAddr string
	if s.bootstrap {
		joinAddr = s.grpcAddr
	} else {
		joinAddr = s.peerGrpcAddr
	}
	client, err := NewGRPCClient(joinAddr)
	if err != nil {
		s.logger.Error("failed to create gRPC client", zap.String("addr", joinAddr), zap.Error(err))
		return
	}
	defer func() {
		if err := client.Close(); err != nil {
			s.logger.Error("failed to close gRPC client", zap.String("addr", joinAddr), zap.Error(err))
		}
	}()

	// join this node to the existing cluster
	joinRequest := &pbkvs.JoinRequest{
		Id:       s.nodeId,
		BindAddr: s.bindAddr,
		GrpcAddr: s.grpcAddr,
		HttpAddr: s.httpAddr,
	}
	if err = client.Join(joinRequest); err != nil {
		s.logger.Error("failed to join node to the cluster", zap.Any("req", joinRequest), zap.Error(err))
		return
	}
}

func (s *Server) Stop() {
	if err := s.grpcGateway.Stop(); err != nil {
		s.logger.Error("failed to stop gRPC gateway", zap.Error(err))
	}

	if err := s.grpcServer.Stop(); err != nil {
		s.logger.Error("failed to stop gRPC server", zap.Error(err))
	}

	if err := s.raftServer.Stop(); err != nil {
		s.logger.Error("failed to stop Raft server", zap.Error(err))
	}
}
