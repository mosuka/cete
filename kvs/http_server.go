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
	"net"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/mash/go-accesslog"
	cetelog "github.com/mosuka/cete/log"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

type HTTPServer struct {
	listener net.Listener
	router   *mux.Router

	grpcClient *GRPCClient

	logger     *zap.Logger
	httpLogger *zap.Logger
}

func NewHTTPServer(httpAddr string, grpcAddr string, logger *zap.Logger) (*HTTPServer, error) {
	grpcClient, err := NewGRPCClient(grpcAddr)
	if err != nil {
		logger.Error("failed to create gRPC client", zap.String("addr", grpcAddr), zap.Error(err))
		return nil, err
	}

	listener, err := net.Listen("tcp", httpAddr)
	if err != nil {
		logger.Error("failed to create listener", zap.String("addr", httpAddr), zap.Error(err))
		return nil, err
	}

	router := mux.NewRouter()
	router.StrictSlash(true)

	router.Handle("/", NewRootHandler(logger)).Methods("GET")
	router.Handle("/store/{path:.*}", NewPutHandler(grpcClient, logger)).Methods("PUT")
	router.Handle("/store/{path:.*}", NewGetHandler(grpcClient, logger)).Methods("GET")
	router.Handle("/store/{path:.*}", NewDeleteHandler(grpcClient, logger)).Methods("DELETE")
	router.Handle("/metrics", promhttp.Handler()).Methods("GET")

	return &HTTPServer{
		listener:   listener,
		router:     router,
		grpcClient: grpcClient,
		logger:     logger,
		httpLogger: logger.Named("http"),
	}, nil
}

func (s *HTTPServer) Start() error {
	err := http.Serve(
		s.listener,
		accesslog.NewLoggingHandler(
			s.router,
			cetelog.HTTPLogger{
				Logger: s.httpLogger,
			},
		),
	)
	if err != nil {
		s.logger.Error("failed to start listener", zap.String("addr", s.listener.Addr().String()), zap.Error(err))
		return err
	}

	return nil
}

func (s *HTTPServer) Stop() error {
	err := s.listener.Close()
	if err != nil {
		s.logger.Error("failed to close listener", zap.String("addr", s.listener.Addr().String()), zap.Error(err))
		return err
	}

	err = s.grpcClient.Close()
	if err != nil {
		s.logger.Error("failed to close gRPC client", zap.String("addr", s.grpcClient.conn.Target()), zap.Error(err))
		return err
	}

	return nil
}
