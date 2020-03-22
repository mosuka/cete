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

	"github.com/golang/protobuf/ptypes/any"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/hashicorp/raft"
	"github.com/mosuka/cete/client"
	"github.com/mosuka/cete/errors"
	"github.com/mosuka/cete/marshaler"
	"github.com/mosuka/cete/metric"
	"github.com/mosuka/cete/protobuf"
	"github.com/prometheus/common/expfmt"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GRPCService struct {
	raftServer *RaftServer
	logger     *zap.Logger

	watchMutex sync.RWMutex
	watchChans map[chan protobuf.WatchResponse]struct{}
}

func NewGRPCService(raftServer *RaftServer, logger *zap.Logger) (*GRPCService, error) {
	return &GRPCService{
		raftServer: raftServer,
		logger:     logger,

		watchChans: make(map[chan protobuf.WatchResponse]struct{}),
	}, nil
}

func (s *GRPCService) Join(ctx context.Context, req *protobuf.JoinRequest) (*empty.Empty, error) {
	resp := &empty.Empty{}

	if s.raftServer.raft.State() != raft.Leader {
		// forward to leader node
		timeout := 1 * time.Second
		leaderAddr, err := s.raftServer.LeaderAddress(timeout)
		if err != nil {
			s.logger.Error("failed to get leader address", zap.Duration("timeout", timeout), zap.Error(err))
			return resp, status.Error(codes.Internal, err.Error())
		}

		c, err := client.NewGRPCClient(string(leaderAddr))
		if err != nil {
			s.logger.Error("failed to create gRPC client", zap.String("leaderAddr", string(leaderAddr)), zap.Error(err))
			return resp, status.Error(codes.Internal, err.Error())
		}
		defer func() {
			err := c.Close()
			if err != nil {
				s.logger.Error("failed to close gRPC client", zap.String("leaderAddr", string(leaderAddr)), zap.Error(err))
			}
		}()

		err = c.Join(req)
		if err != nil {
			s.logger.Error("failed to forward request", zap.String("leaderAddr", string(leaderAddr)), zap.Error(err))
			return resp, status.Error(codes.Internal, err.Error())
		}

		return resp, nil
	}

	err := s.raftServer.Join(req)
	if err != nil {
		switch err {
		case errors.ErrNodeAlreadyExists:
			s.logger.Debug("node already exists", zap.Any("req", req), zap.Error(err))
		default:
			s.logger.Error("failed to join node to the cluster", zap.String("id", req.Id), zap.Error(err))
			return resp, status.Error(codes.Internal, err.Error())
		}
	} else {
		joinReqAny := &any.Any{}
		if err := marshaler.UnmarshalAny(req, joinReqAny); err != nil {
			s.logger.Error("failed to unmarshal request to the watch data", zap.String("id", req.Id), zap.String("err", err.Error()))
		} else {
			watchResp := &protobuf.WatchResponse{
				Event: protobuf.WatchResponse_JOIN,
				Data:  joinReqAny,
			}
			for c := range s.watchChans {
				c <- *watchResp
			}
		}
	}

	return resp, nil
}

func (s *GRPCService) Leave(ctx context.Context, req *protobuf.LeaveRequest) (*empty.Empty, error) {
	resp := &empty.Empty{}

	if s.raftServer.raft.State() != raft.Leader {
		// forward to leader node
		timeout := 1 * time.Second
		leaderAddr, err := s.raftServer.LeaderAddress(timeout)
		if err != nil {
			s.logger.Error("failed to get leader address", zap.Duration("timeout", timeout), zap.Error(err))
			return resp, status.Error(codes.Internal, err.Error())
		}

		c, err := client.NewGRPCClient(string(leaderAddr))
		if err != nil {
			s.logger.Error("failed to create gRPC client", zap.String("leaderAddr", string(leaderAddr)), zap.Error(err))
			return resp, status.Error(codes.Internal, err.Error())
		}
		defer func() {
			err := c.Close()
			if err != nil {
				s.logger.Error("failed to close gRPC client", zap.String("leaderAddr", string(leaderAddr)), zap.Error(err))
			}
		}()

		err = c.Leave(req)
		if err != nil {
			s.logger.Error("failed to forward request", zap.String("leaderAddr", string(leaderAddr)), zap.Error(err))
			return resp, status.Error(codes.Internal, err.Error())
		}

		return resp, nil
	}

	err := s.raftServer.Leave(req)
	if err != nil {
		s.logger.Error("failed to leave node from the cluster", zap.Any("req", req), zap.Error(err))
		return resp, status.Error(codes.Internal, err.Error())
	}

	// notify
	leaveReqAny := &any.Any{}
	if err := marshaler.UnmarshalAny(req, leaveReqAny); err != nil {
		s.logger.Error("failed to unmarshal request to the watch data", zap.Any("req", req), zap.String("err", err.Error()))
	} else {
		watchResp := &protobuf.WatchResponse{
			Event: protobuf.WatchResponse_LEAVE,
			Data:  leaveReqAny,
		}
		for c := range s.watchChans {
			c <- *watchResp
		}
	}

	return resp, nil
}

func (s *GRPCService) Node(ctx context.Context, req *empty.Empty) (*protobuf.NodeResponse, error) {
	resp := &protobuf.NodeResponse{}

	var err error

	resp, err = s.raftServer.Node()
	if err != nil {
		s.logger.Error("failed to get node info", zap.String("err", err.Error()))
		return resp, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}

func (s *GRPCService) Cluster(ctx context.Context, req *empty.Empty) (*protobuf.ClusterResponse, error) {
	resp := &protobuf.ClusterResponse{}

	var err error

	resp, err = s.raftServer.Cluster()
	if err != nil {
		s.logger.Error("failed to get cluster info", zap.String("err", err.Error()))
		return resp, status.Error(codes.Internal, err.Error())
	}

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

func (s *GRPCService) Put(ctx context.Context, req *protobuf.PutRequest) (*empty.Empty, error) {
	resp := &empty.Empty{}

	if s.raftServer.raft.State() != raft.Leader {
		// forward to leader node
		timeout := 1 * time.Second
		leaderAddr, err := s.raftServer.LeaderAddress(timeout)
		if err != nil {
			s.logger.Error("failed to get leader address", zap.Duration("timeout", timeout), zap.Error(err))
			return resp, status.Error(codes.Internal, err.Error())
		}

		c, err := client.NewGRPCClient(string(leaderAddr))
		if err != nil {
			s.logger.Error("failed to create gRPC client", zap.String("leaderAddr", string(leaderAddr)), zap.Error(err))
			return resp, status.Error(codes.Internal, err.Error())
		}
		defer func() {
			err := c.Close()
			if err != nil {
				s.logger.Error("failed to close gRPC client", zap.String("leaderAddr", string(leaderAddr)), zap.Error(err))
			}
		}()

		err = c.Put(req)
		if err != nil {
			s.logger.Error("failed to forward request", zap.String("leaderAddr", string(leaderAddr)), zap.Error(err))
			return resp, status.Error(codes.Internal, err.Error())
		}

		return resp, nil
	}

	// put value by key
	err := s.raftServer.Set(req)
	if err != nil {
		s.logger.Error("failed to put data", zap.Any("req", req), zap.Error(err))
		return resp, status.Error(codes.Internal, err.Error())
	}

	// notify
	putReqAny := &any.Any{}
	if err := marshaler.UnmarshalAny(req, putReqAny); err != nil {
		s.logger.Error("failed to unmarshal request to the watch data", zap.Any("req", req), zap.String("err", err.Error()))
	} else {
		watchResp := &protobuf.WatchResponse{
			Event: protobuf.WatchResponse_PUT,
			Data:  putReqAny,
		}
		for c := range s.watchChans {
			c <- *watchResp
		}
	}

	return resp, nil
}

func (s *GRPCService) Delete(ctx context.Context, req *protobuf.DeleteRequest) (*empty.Empty, error) {
	resp := &empty.Empty{}

	if s.raftServer.raft.State() != raft.Leader {
		// forward to leader node
		timeout := 1 * time.Second
		leaderAddr, err := s.raftServer.LeaderAddress(timeout)
		if err != nil {
			s.logger.Error("failed to get leader address", zap.Duration("timeout", timeout), zap.Error(err))
			return resp, status.Error(codes.Internal, err.Error())
		}

		c, err := client.NewGRPCClient(string(leaderAddr))
		if err != nil {
			s.logger.Error("failed to create gRPC client", zap.String("leaderAddr", string(leaderAddr)), zap.Error(err))
			return resp, status.Error(codes.Internal, err.Error())
		}
		defer func() {
			err := c.Close()
			if err != nil {
				s.logger.Error("failed to close gRPC client", zap.String("leaderAddr", string(leaderAddr)), zap.Error(err))
			}
		}()

		err = c.Delete(req)
		if err != nil {
			s.logger.Error("failed to forward request", zap.String("leaderAddr", string(leaderAddr)), zap.Error(err))
			return resp, status.Error(codes.Internal, err.Error())
		}

		return resp, nil
	}

	// delete value by key
	err := s.raftServer.Delete(req)
	if err != nil {
		s.logger.Error("failed to delete data", zap.String("key", req.Key), zap.Error(err))
		return resp, status.Error(codes.Internal, err.Error())
	}

	// notify
	deleteReqAny := &any.Any{}
	if err := marshaler.UnmarshalAny(req, deleteReqAny); err != nil {
		s.logger.Error("failed to unmarshal request to the watch data", zap.String("key", req.Key), zap.Error(err))
	} else {
		watchResp := &protobuf.WatchResponse{
			Event: protobuf.WatchResponse_DELETE,
			Data:  deleteReqAny,
		}
		for c := range s.watchChans {
			c <- *watchResp
		}
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
