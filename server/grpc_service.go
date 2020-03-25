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
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/hashicorp/raft"
	"github.com/mosuka/cete/client"
	"github.com/mosuka/cete/errors"
	"github.com/mosuka/cete/metric"
	"github.com/mosuka/cete/protobuf"
	"github.com/prometheus/common/expfmt"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GRPCService struct {
	raftServer   *RaftServer
	certFile     string
	certHostname string
	logger       *zap.Logger

	watchMutex sync.RWMutex
	watchChans map[chan protobuf.WatchResponse]struct{}

	watchClusterStopCh chan struct{}
	watchClusterDoneCh chan struct{}
}

func NewGRPCService(raftServer *RaftServer, certFile string, certHostname string, logger *zap.Logger) (*GRPCService, error) {
	return &GRPCService{
		raftServer:   raftServer,
		certFile:     certFile,
		certHostname: certHostname,
		logger:       logger,

		watchChans: make(map[chan protobuf.WatchResponse]struct{}),

		watchClusterStopCh: make(chan struct{}),
		watchClusterDoneCh: make(chan struct{}),
	}, nil
}

func (s *GRPCService) Start() error {
	go func() {
		s.startWatchCluster(500 * time.Millisecond)
	}()

	s.logger.Info("gRPC service started")
	return nil
}

func (s *GRPCService) Stop() error {
	s.stopWatchCluster()

	s.logger.Info("gRPC service stopped")
	return nil
}

func (s *GRPCService) startWatchCluster(checkInterval time.Duration) {
	s.logger.Info("start to update cluster info")

	defer func() {
		close(s.watchClusterDoneCh)
	}()

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	timeout := 60 * time.Second
	if err := s.raftServer.WaitForDetectLeader(timeout); err != nil {
		if err == errors.ErrTimeout {
			s.logger.Error("leader detection timed out", zap.Duration("timeout", timeout), zap.Error(err))
		} else {
			s.logger.Error("failed to detect leader", zap.Error(err))
		}
	}

	for {
		select {
		case <-s.watchClusterStopCh:
			s.logger.Info("received a request to stop updating a cluster")
			return
		case <-s.raftServer.raft.LeaderCh():
			s.logger.Info("became a leader", zap.String("leaderAddr", string(s.raftServer.raft.Leader())))
		case event := <-s.raftServer.applyCh:
			s.logger.Info("receive an event", zap.Any("event", event))
			watchResp := &protobuf.WatchResponse{
				Event: event,
			}
			for c := range s.watchChans {
				c <- *watchResp
			}
			s.logger.Info("send an event", zap.Any("event", event))
		case <-ticker.C:
			s.logger.Debug("tick")
		}
	}
}

func (s *GRPCService) stopWatchCluster() {
	if s.watchClusterStopCh != nil {
		s.logger.Info("send a request to stop updating a cluster")
		close(s.watchClusterStopCh)
	}

	s.logger.Info("wait for the cluster watching to stop")
	<-s.watchClusterDoneCh
	s.logger.Info("the cluster watching has been stopped")
}

func (s *GRPCService) Join(ctx context.Context, req *protobuf.JoinRequest) (*empty.Empty, error) {
	resp := &empty.Empty{}

	if s.raftServer.raft.State() != raft.Leader {
		// forward to leader node
		clusterResp, err := s.Cluster(ctx, &empty.Empty{})
		if err != nil {
			s.logger.Error("failed to get cluster info", zap.Error(err))
			return resp, status.Error(codes.Internal, err.Error())
		}

		leaderNode, ok := clusterResp.Cluster.Nodes[clusterResp.Cluster.Leader]
		if !ok {
			err = errors.ErrNotFoundLeader
			s.logger.Error("failed to get leader gRPC address", zap.Error(err))
			return resp, status.Error(codes.Internal, err.Error())
		}

		c, err := client.NewGRPCClient(leaderNode.Metadata.GrpcAddr)
		if err != nil {
			s.logger.Error("failed to create gRPC client", zap.String("grpc_addr", leaderNode.Metadata.GrpcAddr), zap.Error(err))
			return resp, status.Error(codes.Internal, err.Error())
		}
		defer func() {
			err := c.Close()
			if err != nil {
				s.logger.Error("failed to close gRPC client", zap.String("grpc_addr", leaderNode.Metadata.GrpcAddr), zap.Error(err))
			}
		}()

		err = c.Join(req)
		if err != nil {
			s.logger.Error("failed to forward request", zap.String("grpc_addr", leaderNode.Metadata.GrpcAddr), zap.Error(err))
			return resp, status.Error(codes.Internal, err.Error())
		}

		return resp, nil
	}

	err := s.raftServer.Join(req.Id, req.Node)
	if err != nil {
		switch err {
		case errors.ErrNodeAlreadyExists:
			s.logger.Debug("node already exists", zap.Any("req", req), zap.Error(err))
		default:
			s.logger.Error("failed to join node to the cluster", zap.String("id", req.Id), zap.Error(err))
			return resp, status.Error(codes.Internal, err.Error())
		}
	}

	return resp, nil
}

func (s *GRPCService) Leave(ctx context.Context, req *protobuf.LeaveRequest) (*empty.Empty, error) {
	resp := &empty.Empty{}

	if s.raftServer.raft.State() != raft.Leader {
		// forward to leader node
		clusterResp, err := s.Cluster(ctx, &empty.Empty{})
		if err != nil {
			s.logger.Error("failed to get cluster info", zap.Error(err))
			return resp, status.Error(codes.Internal, err.Error())
		}

		leaderNode, ok := clusterResp.Cluster.Nodes[clusterResp.Cluster.Leader]
		if !ok {
			err = errors.ErrNotFoundLeader
			s.logger.Error("failed to get leader gRPC address", zap.Error(err))
			return resp, status.Error(codes.Internal, err.Error())
		}

		c, err := client.NewGRPCClient(leaderNode.Metadata.GrpcAddr)
		if err != nil {
			s.logger.Error("failed to create gRPC client", zap.String("grpc_addr", leaderNode.Metadata.GrpcAddr), zap.Error(err))
			return resp, status.Error(codes.Internal, err.Error())
		}
		defer func() {
			err := c.Close()
			if err != nil {
				s.logger.Error("failed to close gRPC client", zap.String("grpc_addr", leaderNode.Metadata.GrpcAddr), zap.Error(err))
			}
		}()

		err = c.Leave(req)
		if err != nil {
			s.logger.Error("failed to forward request", zap.String("grpc_addr", leaderNode.Metadata.GrpcAddr), zap.Error(err))
			return resp, status.Error(codes.Internal, err.Error())
		}

		return resp, nil
	}

	err := s.raftServer.Leave(req.Id)
	if err != nil {
		s.logger.Error("failed to leave node from the cluster", zap.Any("req", req), zap.Error(err))
		return resp, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}

func (s *GRPCService) Node(ctx context.Context, req *empty.Empty) (*protobuf.NodeResponse, error) {
	resp := &protobuf.NodeResponse{}

	node, err := s.raftServer.Node()
	if err != nil {
		s.logger.Error("failed to get node info", zap.String("err", err.Error()))
		return resp, status.Error(codes.Internal, err.Error())
	}

	resp.Node = node

	return resp, nil
}

func (s *GRPCService) Cluster(ctx context.Context, req *empty.Empty) (*protobuf.ClusterResponse, error) {
	resp := &protobuf.ClusterResponse{}

	cluster := &protobuf.Cluster{}

	nodes, err := s.raftServer.Nodes()
	if err != nil {
		s.logger.Error("failed to get cluster info", zap.String("err", err.Error()))
		return resp, status.Error(codes.Internal, err.Error())
	}

	for _, node := range nodes {
		c, err := client.NewGRPCClient(node.Metadata.GrpcAddr)
		if err != nil {
			node.State = raft.Shutdown.String()
			s.logger.Error("failed to connect server", zap.String("grpc_addr", node.Metadata.GrpcAddr), zap.String("err", err.Error()))
			continue
		}
		nodeResp, err := c.Node()
		if err != nil {
			node.State = raft.Shutdown.String()
			s.logger.Error("failed to get node info", zap.String("grpc_addr", node.Metadata.GrpcAddr), zap.String("err", err.Error()))
		} else {
			node.State = nodeResp.Node.State
		}
		_ = c.Close()
	}
	cluster.Nodes = nodes

	serverID, err := s.raftServer.LeaderID(60 * time.Second)
	if err != nil {
		s.logger.Error("failed to get cluster info", zap.String("err", err.Error()))
		return resp, status.Error(codes.Internal, err.Error())
	}
	cluster.Leader = string(serverID)

	resp.Cluster = cluster

	return resp, nil
}

func (s *GRPCService) Snapshot(ctx context.Context, req *empty.Empty) (*empty.Empty, error) {
	resp := &empty.Empty{}

	err := s.raftServer.Snapshot()
	if err != nil {
		s.logger.Error("failed to snapshot data", zap.String("err", err.Error()))
		return resp, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}

func (s *GRPCService) Get(ctx context.Context, req *protobuf.GetRequest) (*protobuf.GetResponse, error) {
	resp := &protobuf.GetResponse{}

	var err error

	resp, err = s.raftServer.Get(req)
	if err != nil {
		switch err {
		case errors.ErrNotFound:
			s.logger.Debug("key not found", zap.String("key", req.Key), zap.String("err", err.Error()))
			return resp, status.Error(codes.NotFound, err.Error())
		default:
			s.logger.Debug("failed to get data", zap.String("key", req.Key), zap.String("err", err.Error()))
			return resp, status.Error(codes.Internal, err.Error())
		}
	}

	return resp, nil
}

func (s *GRPCService) Set(ctx context.Context, req *protobuf.SetRequest) (*empty.Empty, error) {
	resp := &empty.Empty{}

	if s.raftServer.raft.State() != raft.Leader {
		// forward to leader node
		clusterResp, err := s.Cluster(ctx, &empty.Empty{})
		if err != nil {
			s.logger.Error("failed to get cluster info", zap.Error(err))
			return resp, status.Error(codes.Internal, err.Error())
		}

		leaderNode, ok := clusterResp.Cluster.Nodes[clusterResp.Cluster.Leader]
		if !ok {
			err = errors.ErrNotFoundLeader
			s.logger.Error("failed to get leader gRPC address", zap.Error(err))
			return resp, status.Error(codes.Internal, err.Error())
		}

		c, err := client.NewGRPCClient(leaderNode.Metadata.GrpcAddr)
		if err != nil {
			s.logger.Error("failed to create gRPC client", zap.String("grpc_addr", leaderNode.Metadata.GrpcAddr), zap.Error(err))
			return resp, status.Error(codes.Internal, err.Error())
		}
		defer func() {
			err := c.Close()
			if err != nil {
				s.logger.Error("failed to close gRPC client", zap.String("grpc_addr", leaderNode.Metadata.GrpcAddr), zap.Error(err))
			}
		}()

		err = c.Set(req)
		if err != nil {
			s.logger.Error("failed to forward request", zap.String("grpc_addr", leaderNode.Metadata.GrpcAddr), zap.Error(err))
			return resp, status.Error(codes.Internal, err.Error())
		}

		return resp, nil
	}

	err := s.raftServer.Set(req)
	if err != nil {
		s.logger.Error("failed to put data", zap.Any("req", req), zap.Error(err))
		return resp, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}

func (s *GRPCService) Delete(ctx context.Context, req *protobuf.DeleteRequest) (*empty.Empty, error) {
	resp := &empty.Empty{}

	if s.raftServer.raft.State() != raft.Leader {
		// forward to leader node
		clusterResp, err := s.Cluster(ctx, &empty.Empty{})
		if err != nil {
			s.logger.Error("failed to get cluster info", zap.Error(err))
			return resp, status.Error(codes.Internal, err.Error())
		}

		leaderNode, ok := clusterResp.Cluster.Nodes[clusterResp.Cluster.Leader]
		if !ok {
			err = errors.ErrNotFoundLeader
			s.logger.Error("failed to get leader gRPC address", zap.Error(err))
			return resp, status.Error(codes.Internal, err.Error())
		}

		c, err := client.NewGRPCClient(leaderNode.Metadata.GrpcAddr)
		if err != nil {
			s.logger.Error("failed to create gRPC client", zap.String("grpc_addr", leaderNode.Metadata.GrpcAddr), zap.Error(err))
			return resp, status.Error(codes.Internal, err.Error())
		}
		defer func() {
			err := c.Close()
			if err != nil {
				s.logger.Error("failed to close gRPC client", zap.String("grpc_addr", leaderNode.Metadata.GrpcAddr), zap.Error(err))
			}
		}()

		err = c.Delete(req)
		if err != nil {
			s.logger.Error("failed to forward request", zap.String("grpc_addr", leaderNode.Metadata.GrpcAddr), zap.Error(err))
			return resp, status.Error(codes.Internal, err.Error())
		}

		return resp, nil
	}

	err := s.raftServer.Delete(req)
	if err != nil {
		s.logger.Error("failed to delete data", zap.String("key", req.Key), zap.Error(err))
		return resp, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}

func (s *GRPCService) Watch(req *empty.Empty, server protobuf.KVS_WatchServer) error {
	chans := make(chan protobuf.WatchResponse)

	s.watchMutex.Lock()
	s.watchChans[chans] = struct{}{}
	s.watchMutex.Unlock()

	defer func() {
		s.watchMutex.Lock()
		delete(s.watchChans, chans)
		s.watchMutex.Unlock()
		close(chans)
	}()

	for resp := range chans {
		if err := server.Send(&resp); err != nil {
			s.logger.Error("failed to send watch data", zap.String("event", resp.Event.String()), zap.Error(err))
			return status.Error(codes.Internal, err.Error())
		}
	}

	return nil
}

func (s *GRPCService) Metrics(ctx context.Context, req *empty.Empty) (*protobuf.MetricsResponse, error) {
	resp := &protobuf.MetricsResponse{}

	var err error

	gather, err := metric.Registry.Gather()
	if err != nil {
		s.logger.Error("failed to get gather", zap.Error(err))
	}
	out := &bytes.Buffer{}
	for _, mf := range gather {
		if _, err := expfmt.MetricFamilyToText(out, mf); err != nil {
			s.logger.Error("failed to parse metric family", zap.Error(err))
		}
	}

	resp.Metrics = out.Bytes()

	return resp, nil
}
