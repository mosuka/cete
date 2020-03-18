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
	"github.com/mosuka/cete/metric"
	pbkvs "github.com/mosuka/cete/protobuf/kvs"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type GRPCServer struct {
	address  string
	service  pbkvs.KVSServer
	server   *grpc.Server
	listener net.Listener

	logger *zap.Logger
}

func NewGRPCServer(address string, raftServer *RaftServer, logger *zap.Logger) (*GRPCServer, error) {
	grpcLogger := logger.Named("grpc")

	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(math.MaxInt64),
		grpc.MaxSendMsgSize(math.MaxInt64),
		grpc.StreamInterceptor(
			grpcmiddleware.ChainStreamServer(
				metric.GrpcMetrics.StreamServerInterceptor(),
				grpczap.StreamServerInterceptor(grpcLogger),
			),
		),
		grpc.UnaryInterceptor(
			grpcmiddleware.ChainUnaryServer(
				metric.GrpcMetrics.UnaryServerInterceptor(),
				grpczap.UnaryServerInterceptor(grpcLogger),
			),
		),
	)

	service, err := NewGRPCService(raftServer, logger)
	if err != nil {
		logger.Error("failed to create key value store service", zap.Error(err))
		return nil, err
	}

	pbkvs.RegisterKVSServer(server, service)

	// Initialize all metrics.
	metric.GrpcMetrics.InitializeMetrics(server)

	listener, err := net.Listen("tcp", address)
	if err != nil {
		logger.Error("failed to create listener", zap.String("address", address), zap.Error(err))
		return nil, err
	}

	return &GRPCServer{
		address:  address,
		service:  service,
		server:   server,
		listener: listener,
		logger:   logger,
	}, nil
}

func (s *GRPCServer) Start() error {
	go s.server.Serve(s.listener)

	s.logger.Info("gRPC server started", zap.String("addr", s.address))
	return nil
}

func (s *GRPCServer) Stop() error {
	//s.server.GracefulStop()
	s.server.Stop()

	s.logger.Info("gRPC server stopped", zap.String("addr", s.address))
	return nil
}
