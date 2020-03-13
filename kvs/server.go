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
	"log"

	pbkvs "github.com/mosuka/cete/protobuf/kvs"
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

	logger     *log.Logger
	httpLogger *log.Logger
}

func NewServer(nodeId string, bindAddr string, grpcAddr string, httpAddr string, dataDir string, joinAddr string, logger *log.Logger, httpLogger *log.Logger) (*Server, error) {
	//var err error

	bootstrap := joinAddr == "" || joinAddr == grpcAddr

	raftServer, err := NewRaftServer(nodeId, bindAddr, grpcAddr, httpAddr, dataDir, bootstrap, logger)
	if err != nil {
		return nil, err
	}

	//raftService := raftServer.transport.GetServerService()

	kvsService, err := NewKvsService(raftServer, logger)
	if err != nil {
		return nil, err
	}

	//grpcServer, err := NewGRPCServer(grpcAddr, raftService, kvsService, logger)
	grpcServer, err := NewGRPCServer(grpcAddr, kvsService, logger)
	if err != nil {
		return nil, err
	}

	httpServer, err := NewHTTPServer(httpAddr, grpcAddr, logger, httpLogger)
	if err != nil {
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
		httpLogger: httpLogger,
	}

	return server, nil
}

func (s *Server) Start() {
	go func() {
		err := s.raftServer.Start()
		if err != nil {
			s.logger.Printf("[ERR] %v", err)
			return
		}
	}()
	s.logger.Print("[INFO] Raft server started")

	go func() {
		err := s.grpcServer.Start()
		if err != nil {
			s.logger.Printf("[ERR] %v", err)
			return
		}
	}()
	s.logger.Print("[INFO] gRPC server started")

	go func() {
		err := s.httpServer.Start()
		if err != nil {
			s.logger.Printf("[ERR] %v", err)
			return
		}
	}()
	s.logger.Print("[INFO] HTTP server started")

	if !s.bootstrap {
		// create gRPC client
		client, err := NewGRPCClient(s.joinAddr)
		if err != nil {
			s.logger.Printf("[ERR] %v", err)
			return
		}
		defer func() {
			err := client.Close()
			if err != nil {
				s.logger.Printf("[ERR] %v", err)
			}
		}()

		// join to the existing cluster
		joinRequest := &pbkvs.JoinRequest{
			Id:       s.nodeId,
			BindAddr: s.bindAddr,
			GrpcAddr: s.grpcAddr,
			HttpAddr: s.httpAddr,
		}
		err = client.Join(joinRequest)
		if err != nil {
			s.logger.Printf("[ERR] %v", err)
			return
		}
	}
}

func (s *Server) Stop() {
	err := s.httpServer.Stop()
	if err != nil {
		s.logger.Printf("[ERR] %v", err)
	}

	err = s.grpcServer.Stop()
	if err != nil {
		s.logger.Printf("[ERR] %v", err)
	}

	err = s.raftServer.Stop()
	if err != nil {
		s.logger.Printf("[ERR] %v", err)
	}
}
