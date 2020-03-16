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
	"math"
	"net"

	grpcmiddleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpczap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	pbkvs "github.com/mosuka/cete/protobuf/kvs"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type GRPCServer struct {
	server   *grpc.Server
	listener net.Listener

	logger *zap.Logger
}

//func NewGRPCServer(grpcAddr string, raftService raftgrpc.RaftServiceServer, kvsService pbkvs.KVSServer, logger *log.Logger) (*GRPCServer, error) {
func NewGRPCServer(grpcAddr string, kvsService pbkvs.KVSServer, logger *zap.Logger) (*GRPCServer, error) {
	grpcLogger := logger.Named("grpc")
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(math.MaxInt64),
		grpc.MaxSendMsgSize(math.MaxInt64),
		grpc.StreamInterceptor(
			grpcmiddleware.ChainStreamServer(
				grpcprometheus.StreamServerInterceptor,
				grpczap.StreamServerInterceptor(grpcLogger),
			),
		),
		grpc.UnaryInterceptor(
			grpcmiddleware.ChainUnaryServer(
				grpcprometheus.UnaryServerInterceptor,
				grpczap.UnaryServerInterceptor(grpcLogger),
			),
		),
	)

	//raftgrpc.RegisterRaftServiceServer(server, raftService)
	pbkvs.RegisterKVSServer(server, kvsService)

	listener, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Error("failed to create listener", zap.String("addr", grpcAddr), zap.Error(err))
		return nil, err
	}

	return &GRPCServer{
		server:   server,
		listener: listener,
		logger:   logger,
	}, nil
}

func (s *GRPCServer) Start() error {
	err := s.server.Serve(s.listener)
	if err != nil {
		s.logger.Error("failed to start server", zap.String("addr", s.listener.Addr().String()), zap.Error(err))
		return err
	}

	return nil
}

func (s *GRPCServer) Stop() error {
	//s.server.GracefulStop()
	s.server.Stop()
	s.logger.Info("server stopped")

	return nil
}
