// Copyright (c) 2019 Minoru Osuka
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
	pbkvs "github.com/mosuka/cete/protobuf/kvs"
	"go.uber.org/zap"
)

type Server struct {
	nodeId   string
	bindAddr string
	grpcAddr string
	httpAddr string
	dataDir  string

	bootstrap bool
	joinAddr  string

	raftServer *RaftServer
	grpcServer *GRPCServer
	httpServer *HTTPServer

	logger *zap.Logger
}

func NewServer(nodeId string, bindAddr string, grpcAddr string, httpAddr string, dataDir string, joinAddr string, logger *zap.Logger) (*Server, error) {
	bootstrap := joinAddr == "" || joinAddr == grpcAddr

	raftServer, err := NewRaftServer(nodeId, bindAddr, grpcAddr, httpAddr, dataDir, bootstrap, logger)
	if err != nil {
		logger.Error("failed to create Raft server",
			zap.String("id", nodeId),
			zap.String("bind_addr", bindAddr),
			zap.String("grpc_addr", grpcAddr),
			zap.String("http_addr", httpAddr),
			zap.String("data_dir", dataDir),
			zap.Bool("bootstrap", bootstrap),
			zap.String("err", err.Error()),
		)
		return nil, err
	}

	//raftService := raftServer.transport.GetServerService()

	kvsService, err := NewGRPCService(raftServer, logger)
	if err != nil {
		logger.Error("failed to create key value store service", zap.Error(err))
		return nil, err
	}

	//grpcServer, err := NewGRPCServer(grpcAddr, raftService, kvsService, logger)
	grpcServer, err := NewGRPCServer(grpcAddr, kvsService, logger)
	if err != nil {
		logger.Error("failed to create gRPC server", zap.String("grpc_addr", grpcAddr), zap.Error(err))
		return nil, err
	}

	httpServer, err := NewHTTPServer(httpAddr, grpcAddr, logger)
	if err != nil {
		logger.Error("failed to create HTTP server", zap.String("http_addr", httpAddr), zap.String("grpc_addr", grpcAddr), zap.Error(err))
		return nil, err
	}

	server := &Server{
		nodeId:     nodeId,
		bindAddr:   bindAddr,
		grpcAddr:   grpcAddr,
		httpAddr:   httpAddr,
		dataDir:    dataDir,
		bootstrap:  bootstrap,
		joinAddr:   joinAddr,
		raftServer: raftServer,
		grpcServer: grpcServer,
		httpServer: httpServer,
		logger:     logger,
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

	if err := s.httpServer.Start(); err != nil {
		s.logger.Error("failed to start HTTP server", zap.Error(err))
		return
	}

	if !s.bootstrap {
		// create gRPC client
		client, err := NewGRPCClient(s.joinAddr)
		if err != nil {
			s.logger.Error("failed to create gRPC client", zap.String("addr", s.joinAddr), zap.Error(err))
			return
		}
		defer func() {
			if err := client.Close(); err != nil {
				s.logger.Error("failed to close gRPC client", zap.String("addr", s.joinAddr), zap.Error(err))
			}
		}()

		// join to the existing cluster
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
}

func (s *Server) Stop() {
	if err := s.httpServer.Stop(); err != nil {
		s.logger.Error("failed to stop HTTP server", zap.Error(err))
	}

	if err := s.grpcServer.Stop(); err != nil {
		s.logger.Error("failed to stop gRPC server", zap.Error(err))
	}

	if err := s.raftServer.Stop(); err != nil {
		s.logger.Error("failed to stop Raft server", zap.Error(err))
	}
}
