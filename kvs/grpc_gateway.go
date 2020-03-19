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
	"context"
	"net"
	"net/http"

	"github.com/golang/protobuf/proto"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	pbkvs "github.com/mosuka/cete/protobuf/kvs"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func responseFilter(ctx context.Context, w http.ResponseWriter, resp proto.Message) error {
	switch resp.(type) {
	case *pbkvs.GetResponse:
		if r, ok := resp.(*pbkvs.GetResponse); ok {
			w.Header().Set("Content-Type", http.DetectContentType(r.Value))
		}
	case *pbkvs.MetricsResponse:
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	default:
		w.Header().Set("Content-Type", DefaultContentType)
	}

	return nil
}

type GRPCGateway struct {
	grpcGatewayAddr string
	grpcAddr        string

	cancel   context.CancelFunc
	listener net.Listener
	mux      *runtime.ServeMux

	logger *zap.Logger
}

func NewGRPCGateway(grpcGatewayAddr string, grpcAddr string, logger *zap.Logger) (*GRPCGateway, error) {
	baseCtx := context.TODO()
	ctx, cancel := context.WithCancel(baseCtx)

	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, new(CeteMarshaler)),
		runtime.WithForwardResponseOption(responseFilter),
	)
	opts := []grpc.DialOption{grpc.WithInsecure()}

	err := pbkvs.RegisterKVSHandlerFromEndpoint(ctx, mux, grpcAddr, opts)
	if err != nil {
		logger.Error("failed to register KVS handler from endpoint", zap.Error(err))
		return nil, err
	}

	listener, err := net.Listen("tcp", grpcGatewayAddr)
	if err != nil {
		logger.Error("failed to create key value store service", zap.Error(err))
		return nil, err
	}

	return &GRPCGateway{
		grpcGatewayAddr: grpcGatewayAddr,
		grpcAddr:        grpcAddr,
		listener:        listener,
		mux:             mux,
		cancel:          cancel,
		logger:          logger,
	}, nil
}

func (s *GRPCGateway) Start() error {
	go http.Serve(s.listener, s.mux)

	s.logger.Info("gRPC gateway started", zap.String("addr", s.grpcGatewayAddr))
	return nil
}

func (s *GRPCGateway) Stop() error {
	defer s.cancel()

	err := s.listener.Close()
	if err != nil {
		s.logger.Error("failed to close listener", zap.String("addr", s.listener.Addr().String()), zap.Error(err))
	}

	s.logger.Info("gRPC gateway stopped", zap.String("addr", s.grpcGatewayAddr))
	return nil
}
