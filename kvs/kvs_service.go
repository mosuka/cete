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
	"context"
	"log"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes/any"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/hashicorp/raft"
	"github.com/mosuka/cete/errors"
	"github.com/mosuka/cete/protobuf"
	pbkvs "github.com/mosuka/cete/protobuf/kvs"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type KvsService struct {
	raftServer *RaftServer
	logger     *log.Logger

	watchMutex sync.RWMutex
	watchChans map[chan pbkvs.WatchResponse]struct{}
}

func NewKvsService(raftServer *RaftServer, logger *log.Logger) (*KvsService, error) {
	return &KvsService{
		raftServer: raftServer,
		logger:     logger,

		watchChans: make(map[chan pbkvs.WatchResponse]struct{}),
	}, nil
}

func (s *KvsService) Join(ctx context.Context, req *pbkvs.JoinRequest) (*empty.Empty, error) {
	s.logger.Printf("[INFO] %v", req)

	resp := &empty.Empty{}

	if s.raftServer.raft.State() != raft.Leader {
		// forward to leader node
		leaderAddr, err := s.raftServer.LeaderAddress(1 * time.Second)
		if err != nil {
			s.logger.Printf("[ERR] %v", err)
			return resp, status.Error(codes.Internal, err.Error())
		}

		client, err := NewGRPCClient(string(leaderAddr))
		if err != nil {
			s.logger.Printf("[ERR] %v", err)
			return resp, status.Error(codes.Internal, err.Error())
		}
		defer func() {
			err := client.Close()
			if err != nil {
				s.logger.Printf("[ERR] %v", err)
			}
		}()

		err = client.Join(req)
		if err != nil {
			s.logger.Printf("[ERR] %v", err)
			return resp, status.Error(codes.Internal, err.Error())
		}

		return resp, nil
	}

	err := s.raftServer.Join(req)
	if err != nil {
		return resp, status.Error(codes.Internal, err.Error())
	}

	// notify
	joinReqAny := &any.Any{}
	if err := protobuf.UnmarshalAny(req, joinReqAny); err != nil {
		s.logger.Printf("[ERR] %v", err)
	} else {
		watchResp := &pbkvs.WatchResponse{
			Event: pbkvs.WatchResponse_JOIN,
			Data:  joinReqAny,
		}
		for c := range s.watchChans {
			c <- *watchResp
		}
	}

	return resp, nil
}

func (s *KvsService) Leave(ctx context.Context, req *pbkvs.LeaveRequest) (*empty.Empty, error) {
	s.logger.Printf("[INFO] leave %v", req)

	resp := &empty.Empty{}

	if s.raftServer.raft.State() != raft.Leader {
		// forward to leader node
		leaderAddr, err := s.raftServer.LeaderAddress(1 * time.Second)
		if err != nil {
			s.logger.Printf("[ERR] %v", err)
			return resp, status.Error(codes.Internal, err.Error())
		}

		client, err := NewGRPCClient(string(leaderAddr))
		if err != nil {
			s.logger.Printf("[ERR] %v", err)
			return resp, status.Error(codes.Internal, err.Error())
		}
		defer func() {
			err := client.Close()
			if err != nil {
				s.logger.Printf("[ERR] %v", err)
			}
		}()

		err = client.Leave(req)
		if err != nil {
			s.logger.Printf("[ERR] %v", err)
			return resp, status.Error(codes.Internal, err.Error())
		}

		return resp, nil
	}

	err := s.raftServer.Leave(req)
	if err != nil {
		return resp, status.Error(codes.Internal, err.Error())
	}

	// notify
	leaveReqAny := &any.Any{}
	if err := protobuf.UnmarshalAny(req, leaveReqAny); err != nil {
		s.logger.Printf("[ERR] %v", err)
	} else {
		watchResp := &pbkvs.WatchResponse{
			Event: pbkvs.WatchResponse_LEAVE,
			Data:  leaveReqAny,
		}
		for c := range s.watchChans {
			c <- *watchResp
		}
	}

	return resp, nil
}

func (s *KvsService) Node(ctx context.Context, req *empty.Empty) (*pbkvs.NodeResponse, error) {
	s.logger.Printf("[INFO] get node %v", req)

	resp := &pbkvs.NodeResponse{}

	var err error

	resp, err = s.raftServer.Node()
	if err != nil {
		return resp, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}

func (s *KvsService) Cluster(ctx context.Context, req *empty.Empty) (*pbkvs.ClusterResponse, error) {
	s.logger.Printf("[INFO] get cluster %v", req)

	resp := &pbkvs.ClusterResponse{}

	var err error

	resp, err = s.raftServer.Cluster()
	if err != nil {
		return resp, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}

func (s *KvsService) Snapshot(ctx context.Context, req *empty.Empty) (*empty.Empty, error) {
	s.logger.Printf("[INFO] %v", req)

	resp := &empty.Empty{}

	err := s.raftServer.Snapshot()
	if err != nil {
		return resp, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}

func (s *KvsService) Get(ctx context.Context, req *pbkvs.GetRequest) (*pbkvs.GetResponse, error) {
	start := time.Now()
	defer RecordMetrics(start, "get")

	s.logger.Printf("[INFO] get %v", req)

	resp := &pbkvs.GetResponse{}

	var err error

	resp, err = s.raftServer.Get(req)
	if err != nil {
		switch err {
		case errors.ErrNotFound:
			return resp, status.Error(codes.NotFound, err.Error())
		default:
			return resp, status.Error(codes.Internal, err.Error())
		}
	}

	return resp, nil
}

func (s *KvsService) Put(ctx context.Context, req *pbkvs.PutRequest) (*empty.Empty, error) {
	start := time.Now()
	defer RecordMetrics(start, "put")

	resp := &empty.Empty{}

	if s.raftServer.raft.State() != raft.Leader {
		// forward to leader node
		leaderAddr, err := s.raftServer.LeaderAddress(1 * time.Second)
		if err != nil {
			s.logger.Printf("[ERR] %v", err)
			return resp, status.Error(codes.Internal, err.Error())
		}

		client, err := NewGRPCClient(string(leaderAddr))
		if err != nil {
			s.logger.Printf("[ERR] %v", err)
			return resp, status.Error(codes.Internal, err.Error())
		}
		defer func() {
			err := client.Close()
			if err != nil {
				s.logger.Printf("[ERR] %v", err)
			}
		}()

		err = client.Put(req)
		if err != nil {
			s.logger.Printf("[ERR] %v", err)
			return resp, status.Error(codes.Internal, err.Error())
		}

		return resp, nil
	}

	// put value by key
	err := s.raftServer.Set(req)
	if err != nil {
		return resp, status.Error(codes.Internal, err.Error())
	}

	// notify
	putReqAny := &any.Any{}
	if err := protobuf.UnmarshalAny(req, putReqAny); err != nil {
		s.logger.Printf("[ERR] %v", err)
	} else {
		watchResp := &pbkvs.WatchResponse{
			Event: pbkvs.WatchResponse_PUT,
			Data:  putReqAny,
		}
		for c := range s.watchChans {
			c <- *watchResp
		}
	}

	return resp, nil
}

func (s *KvsService) Delete(ctx context.Context, req *pbkvs.DeleteRequest) (*empty.Empty, error) {
	start := time.Now()
	defer RecordMetrics(start, "delete")

	s.logger.Printf("[INFO] delete %v", req)

	resp := &empty.Empty{}

	// delete value by key
	err := s.raftServer.Delete(req)
	if err != nil {
		return resp, status.Error(codes.Internal, err.Error())
	}

	// notify
	deleteReqAny := &any.Any{}
	if err := protobuf.UnmarshalAny(req, deleteReqAny); err != nil {
		s.logger.Printf("[ERR] %v", err)
	} else {
		watchResp := &pbkvs.WatchResponse{
			Event: pbkvs.WatchResponse_DELETE,
			Data:  deleteReqAny,
		}
		for c := range s.watchChans {
			c <- *watchResp
		}
	}

	return resp, nil
}

func (s *KvsService) Watch(req *empty.Empty, server pbkvs.KVS_WatchServer) error {
	chans := make(chan pbkvs.WatchResponse)

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
		err := server.Send(&resp)
		if err != nil {
			s.logger.Printf("[ERR] %v", err)
			return status.Error(codes.Internal, err.Error())
		}
	}

	return nil
}
