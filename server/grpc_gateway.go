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

package server

import (
	"context"
	"google.golang.org/grpc/keepalive"
	"math"
	"net"
	"net/http"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/mosuka/cete/marshaler"
	"github.com/mosuka/cete/protobuf"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func responseFilter(ctx context.Context, w http.ResponseWriter, resp proto.Message) error {
	switch resp.(type) {
	case *protobuf.GetResponse:
		if r, ok := resp.(*protobuf.GetResponse); ok {
			w.Header().Set("Content-Type", http.DetectContentType(r.Value))
		}
	case *protobuf.MetricsResponse:
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	default:
		w.Header().Set("Content-Type", marshaler.DefaultContentType)
	}

	return nil
}

type GRPCGateway struct {
	grpcGatewayAddr string
	grpcAddr        string

	cancel   context.CancelFunc
	listener net.Listener
	mux      *runtime.ServeMux

	certFile string
	keyFile  string

	logger *zap.Logger
}

func NewGRPCGateway(grpcGatewayAddr string, grpcAddr string, certFile string, keyFile string, certHostname string, logger *zap.Logger) (*GRPCGateway, error) {
	dialOpts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(math.MaxInt64),
			grpc.MaxCallRecvMsgSize(math.MaxInt64),
		),
		grpc.WithKeepaliveParams(
			keepalive.ClientParameters{
				Time:                1 * time.Second,
				Timeout:             5 * time.Second,
				PermitWithoutStream: true,
			},
		),
	}

	baseCtx := context.TODO()
	ctx, cancel := context.WithCancel(baseCtx)

	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, new(marshaler.CeteMarshaler)),
		runtime.WithForwardResponseOption(responseFilter),
	)

	// TODO: TLS support for gRPC will be done later.
	dialOpts = append(dialOpts, grpc.WithInsecure())
	//if certFile == "" {
	//	dialOpts = append(dialOpts, grpc.WithInsecure())
	//} else {
	//	creds, err := credentials.NewClientTLSFromFile(certFile, certHostname)
	//	if err != nil {
	//		return nil, err
	//	}
	//	dialOpts = append(dialOpts, grpc.WithTransportCredentials(creds))
	//}

	err := protobuf.RegisterKVSHandlerFromEndpoint(ctx, mux, grpcAddr, dialOpts)
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
		certFile:        certFile,
		keyFile:         keyFile,
		logger:          logger,
	}, nil
}

func (s *GRPCGateway) Start() error {
	if s.certFile == "" && s.keyFile == "" {
		go func() {
			_ = http.Serve(s.listener, s.mux)
		}()
	} else {
		go func() {
			_ = http.ServeTLS(s.listener, s.mux, s.certFile, s.keyFile)
		}()
	}

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
