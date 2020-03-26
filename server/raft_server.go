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
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"time"

	raftbadgerdb "github.com/bbva/raft-badger"
	"github.com/dgraph-io/badger/v2"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/hashicorp/raft"
	"github.com/mosuka/cete/errors"
	"github.com/mosuka/cete/marshaler"
	"github.com/mosuka/cete/protobuf"
	"go.uber.org/zap"
)

type RaftServer struct {
	nodeId    string
	bindAddr  string
	dataDir   string
	bootstrap bool
	logger    *zap.Logger

	fsm *RaftFSM

	transport *raft.NetworkTransport
	raft      *raft.Raft

	watchClusterStopCh chan struct{}
	watchClusterDoneCh chan struct{}

	applyCh chan *protobuf.Event
}

func NewRaftServer(nodeId string, bindAddr string, dataDir string, bootstrap bool, logger *zap.Logger) (*RaftServer, error) {
	fsmPath := filepath.Join(dataDir, "kvs")
	fsm, err := NewRaftFSM(fsmPath, logger)
	if err != nil {
		logger.Error("failed to create FSM", zap.String("path", fsmPath), zap.Error(err))
		return nil, err
	}

	return &RaftServer{
		nodeId:    nodeId,
		bindAddr:  bindAddr,
		dataDir:   dataDir,
		bootstrap: bootstrap,
		fsm:       fsm,
		logger:    logger,

		watchClusterStopCh: make(chan struct{}),
		watchClusterDoneCh: make(chan struct{}),

		applyCh: make(chan *protobuf.Event, 1024),
	}, nil
}

func (s *RaftServer) Start() error {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(s.nodeId)
	config.SnapshotThreshold = 1024
	config.LogOutput = ioutil.Discard

	addr, err := net.ResolveTCPAddr("tcp", s.bindAddr)
	if err != nil {
		s.logger.Error("failed to resolve TCP address", zap.String("tcp", s.bindAddr), zap.Error(err))
		return err
	}

	s.transport, err = raft.NewTCPTransport(s.bindAddr, addr, 3, 10*time.Second, ioutil.Discard)
	if err != nil {
		s.logger.Error("failed to create TCP transport", zap.String("tcp", s.bindAddr), zap.Error(err))
		return err
	}

	// create snapshot store
	snapshotStore, err := raft.NewFileSnapshotStore(s.dataDir, 2, ioutil.Discard)
	if err != nil {
		s.logger.Error("failed to create file snapshot store", zap.String("path", s.dataDir), zap.Error(err))
		return err
	}

	logStorePath := filepath.Join(s.dataDir, "raft", "log")
	err = os.MkdirAll(logStorePath, 0755)
	if err != nil {
		s.logger.Fatal(err.Error())
		return err
	}
	logStoreBadgerOpts := badger.DefaultOptions(logStorePath)
	logStoreBadgerOpts.ValueDir = logStorePath
	logStoreBadgerOpts.SyncWrites = false
	logStoreBadgerOpts.Logger = nil
	logStoreOpts := raftbadgerdb.Options{
		Path:          logStorePath,
		BadgerOptions: &logStoreBadgerOpts,
	}
	raftLogStore, err := raftbadgerdb.New(logStoreOpts)
	if err != nil {
		s.logger.Fatal(err.Error())
		return err
	}

	stableStorePath := filepath.Join(s.dataDir, "raft", "stable")
	err = os.MkdirAll(stableStorePath, 0755)
	if err != nil {
		s.logger.Fatal(err.Error())
		return err
	}
	stableStoreBadgerOpts := badger.DefaultOptions(stableStorePath)
	stableStoreBadgerOpts.ValueDir = stableStorePath
	stableStoreBadgerOpts.SyncWrites = false
	stableStoreBadgerOpts.Logger = nil
	stableStoreOpts := raftbadgerdb.Options{
		Path:          stableStorePath,
		BadgerOptions: &stableStoreBadgerOpts,
	}
	raftStableStore, err := raftbadgerdb.New(stableStoreOpts)
	if err != nil {
		s.logger.Fatal(err.Error())
		return err
	}

	// create raft
	s.raft, err = raft.NewRaft(config, s.fsm, raftLogStore, raftStableStore, snapshotStore, s.transport)
	if err != nil {
		s.logger.Error("failed to create raft", zap.Any("config", config), zap.Error(err))
		return err
	}

	if s.bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: s.transport.LocalAddr(),
				},
			},
		}
		s.raft.BootstrapCluster(configuration)
	}

	go func() {
		s.startWatchCluster(500 * time.Millisecond)
	}()

	s.logger.Info("Raft server started", zap.String("addr", s.bindAddr))
	return nil
}

func (s *RaftServer) Stop() error {
	s.applyCh <- nil
	s.logger.Info("apply channel has closed")

	s.stopWatchCluster()

	if err := s.fsm.Close(); err != nil {
		s.logger.Error("failed to close FSM", zap.Error(err))
	}
	s.logger.Info("Raft FSM Closed")

	if future := s.raft.Shutdown(); future.Error() != nil {
		s.logger.Info("failed to shutdown Raft", zap.Error(future.Error()))
	}
	s.logger.Info("Raft has shutdown", zap.String("addr", s.bindAddr))

	return nil
}

func (s *RaftServer) startWatchCluster(checkInterval time.Duration) {
	s.logger.Info("start to update cluster info")

	defer func() {
		close(s.watchClusterDoneCh)
	}()

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	timeout := 60 * time.Second
	if err := s.WaitForDetectLeader(timeout); err != nil {
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
		case <-s.raft.LeaderCh():
			s.logger.Info("became a leader", zap.String("leaderAddr", string(s.raft.Leader())))
		case event := <-s.fsm.applyCh:
			s.applyCh <- event
			//case <-ticker.C:
			//	s.logger.Debug("tick")
		}
	}
}

func (s *RaftServer) stopWatchCluster() {
	if s.watchClusterStopCh != nil {
		s.logger.Info("send a request to stop updating a cluster")
		close(s.watchClusterStopCh)
	}

	s.logger.Info("wait for the cluster update to stop")
	<-s.watchClusterDoneCh
	s.logger.Info("the cluster update has been stopped")
}

func (s *RaftServer) LeaderAddress(timeout time.Duration) (raft.ServerAddress, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-ticker.C:
			leaderAddr := s.raft.Leader()
			if leaderAddr != "" {
				s.logger.Info("detected a leader address", zap.String("addr", string(leaderAddr)))
				return leaderAddr, nil
			}
		case <-timer.C:
			err := errors.ErrTimeout
			s.logger.Error("failed to detect leader address", zap.Error(err))
			return "", err
		}
	}
}

func (s *RaftServer) LeaderID(timeout time.Duration) (raft.ServerID, error) {
	cf := s.raft.GetConfiguration()
	err := cf.Error()
	if err != nil {
		s.logger.Error("failed to get Raft configuration", zap.Error(err))
		return "", err
	}

	leaderAddr, err := s.LeaderAddress(timeout)
	if err != nil {
		s.logger.Error("failed to get leader address", zap.Error(err))
		return "", err
	}

	for _, server := range cf.Configuration().Servers {
		if server.Address == leaderAddr {
			s.logger.Info("detected a leader ID", zap.String("id", string(server.ID)))
			return server.ID, nil
		}
	}

	err = errors.ErrNotFoundLeader
	s.logger.Error("failed to detect leader ID", zap.Error(err))
	return "", err
}

func (s *RaftServer) WaitForDetectLeader(timeout time.Duration) error {
	if _, err := s.LeaderAddress(timeout); err != nil {
		s.logger.Error("failed to wait for detect leader", zap.Error(err))
		return err
	}

	return nil
}

func (s *RaftServer) State() string {
	return s.raft.State().String()
}

func (s *RaftServer) Exist(id string) (bool, error) {
	exist := false

	cf := s.raft.GetConfiguration()
	err := cf.Error()
	if err != nil {
		s.logger.Error("failed to get Raft configuration", zap.Error(err))
		return false, err
	}

	for _, server := range cf.Configuration().Servers {
		if server.ID == raft.ServerID(id) {
			s.logger.Debug("node already joined the cluster", zap.String("id", id))
			exist = true
			break
		}
	}

	return exist, nil
}

func (s *RaftServer) join(id string, metadata *protobuf.Metadata) error {
	data := &protobuf.SetMetadataRequest{
		Id:       id,
		Metadata: metadata,
	}

	dataAny := &any.Any{}
	err := marshaler.UnmarshalAny(data, dataAny)
	if err != nil {
		s.logger.Error("failed to unmarshal request to the command data", zap.String("id", id), zap.Any("metadata", metadata), zap.Error(err))
		return err
	}

	c := &protobuf.Event{
		Type: protobuf.Event_Join,
		Data: dataAny,
	}

	msg, err := proto.Marshal(c)
	if err != nil {
		s.logger.Error("failed to marshal the command into the bytes as message", zap.String("id", id), zap.Any("metadata", metadata), zap.Error(err))
		return err
	}

	f := s.raft.Apply(msg, 10*time.Second)
	if err = f.Error(); err != nil {
		s.logger.Error("failed to apply message", zap.String("id", id), zap.Any("metadata", metadata), zap.Error(err))
		return err
	}

	return nil
}

func (s *RaftServer) Join(id string, node *protobuf.Node) error {
	nodeExists, err := s.Exist(id)
	if err != nil {
		return err
	}

	if nodeExists {
		s.logger.Debug("node already exists", zap.String("id", id), zap.String("bind_addr", node.BindAddr))
	} else {
		if future := s.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(node.BindAddr), 0, 0); future.Error() != nil {
			s.logger.Error("failed to add voter", zap.String("id", id), zap.String("bind_addr", node.BindAddr), zap.Error(future.Error()))
			return future.Error()
		}
		s.logger.Info("node has successfully joined", zap.String("id", id), zap.String("bind_addr", node.BindAddr))
	}

	if err := s.join(id, node.Metadata); err != nil {
		s.logger.Error("failed to set node metadata", zap.String("id", id), zap.Any("metadata", node.Metadata), zap.Error(err))
		return err
	}
	s.logger.Info("node metadata has successfully set", zap.String("id", id), zap.Any("metadata", node.Metadata))

	if nodeExists {
		return errors.ErrNodeAlreadyExists
	} else {
		return nil
	}
}

func (s *RaftServer) leave(id string) error {
	data := &protobuf.DeleteMetadataRequest{
		Id: id,
	}

	dataAny := &any.Any{}
	err := marshaler.UnmarshalAny(data, dataAny)
	if err != nil {
		s.logger.Error("failed to unmarshal request to the command data", zap.String("id", id), zap.Error(err))
		return err
	}

	c := &protobuf.Event{
		Type: protobuf.Event_Leave,
		Data: dataAny,
	}

	msg, err := proto.Marshal(c)
	if err != nil {
		s.logger.Error("failed to marshal the command into the bytes as the message", zap.String("id", id), zap.Error(err))
		return err
	}

	f := s.raft.Apply(msg, 10*time.Second)
	if err = f.Error(); err != nil {
		s.logger.Error("failed to apply the message", zap.String("id", id), zap.Error(err))
		return err
	}

	return nil
}

func (s *RaftServer) Leave(id string) error {
	nodeExists, err := s.Exist(id)
	if err != nil {
		return err
	}

	if nodeExists {
		if future := s.raft.RemoveServer(raft.ServerID(id), 0, 0); future.Error() != nil {
			s.logger.Error("failed to remove server", zap.String("id", id), zap.Error(future.Error()))
			return future.Error()
		}
		s.logger.Info("node has successfully left", zap.String("id", id))
	} else {
		s.logger.Debug("node does not exists", zap.String("id", id))
	}

	if err = s.leave(id); err != nil {
		s.logger.Error("failed to join node", zap.String("id", id), zap.Error(err))
		return err
	}

	return nil
}

func (s *RaftServer) Node() (*protobuf.Node, error) {
	nodes, err := s.Nodes()
	if err != nil {
		return nil, err
	}

	node, ok := nodes[s.nodeId]
	if !ok {
		return nil, errors.ErrNotFound
	}

	node.State = s.State()

	return node, nil
}

func (s *RaftServer) Nodes() (map[string]*protobuf.Node, error) {
	cf := s.raft.GetConfiguration()
	if err := cf.Error(); err != nil {
		s.logger.Error("failed to get Raft configuration", zap.Error(err))
		return nil, err
	}

	nodes := make(map[string]*protobuf.Node, 0)
	for _, server := range cf.Configuration().Servers {
		nodes[string(server.ID)] = &protobuf.Node{
			BindAddr: string(server.Address),
			Metadata: s.fsm.getMetadata(string(server.ID)),
		}
	}

	return nodes, nil
}

func (s *RaftServer) Snapshot() error {
	if future := s.raft.Snapshot(); future.Error() != nil {
		s.logger.Error("failed to snapshot", zap.Error(future.Error()))
		return future.Error()
	}

	return nil
}

func (s *RaftServer) Get(req *protobuf.GetRequest) (*protobuf.GetResponse, error) {
	value, err := s.fsm.Get(req.Key)
	if err != nil {
		s.logger.Error("failed to get", zap.Any("key", req.Key), zap.Error(err))
		return nil, err
	}

	resp := &protobuf.GetResponse{
		Value: value,
	}

	return resp, nil
}

func (s *RaftServer) Set(req *protobuf.SetRequest) error {
	kvpAny := &any.Any{}
	if err := marshaler.UnmarshalAny(req, kvpAny); err != nil {
		s.logger.Error("failed to unmarshal request to the command data", zap.String("key", req.Key), zap.Error(err))
		return err
	}

	c := &protobuf.Event{
		Type: protobuf.Event_Set,
		Data: kvpAny,
	}

	msg, err := proto.Marshal(c)
	if err != nil {
		s.logger.Error("failed to marshal the command into the bytes as the message", zap.String("key", req.Key), zap.Error(err))
		return err
	}

	if future := s.raft.Apply(msg, 10*time.Second); future.Error() != nil {
		s.logger.Error("failed to apply the message", zap.Error(future.Error()))
		return future.Error()
	}

	return nil
}

func (s *RaftServer) Delete(req *protobuf.DeleteRequest) error {
	kvpAny := &any.Any{}
	if err := marshaler.UnmarshalAny(req, kvpAny); err != nil {
		s.logger.Error("failed to unmarshal request to the command data", zap.String("key", req.Key), zap.Error(err))
		return err
	}

	c := &protobuf.Event{
		Type: protobuf.Event_Delete,
		Data: kvpAny,
	}

	msg, err := proto.Marshal(c)
	if err != nil {
		s.logger.Error("failed to marshal the command into the bytes as the message", zap.String("key", req.Key), zap.Error(err))
		return err
	}

	if future := s.raft.Apply(msg, 10*time.Second); future.Error() != nil {
		s.logger.Error("failed to unmarshal request to the command data", zap.String("key", req.Key), zap.Error(future.Error()))
		return future.Error()
	}

	return nil
}
