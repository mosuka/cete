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
	"io/ioutil"
	"net"
	"path/filepath"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	ceteerrors "github.com/mosuka/cete/errors"
	"github.com/mosuka/cete/protobuf"
	pbkvs "github.com/mosuka/cete/protobuf/kvs"
	"go.uber.org/zap"
)

type RaftServer struct {
	nodeId    string
	bindAddr  string
	grpcAddr  string
	httpAddr  string
	dataDir   string
	bootstrap bool
	logger    *zap.Logger

	fsm *RaftFSM

	transport *raft.NetworkTransport
	raft      *raft.Raft

	updateClusterStopCh chan struct{}
	updateClusterDoneCh chan struct{}
	updateClusterMutex  sync.RWMutex

	updateNodeStopCh chan struct{}
	updateNodeDoneCh chan struct{}
	updateNodeMutex  sync.RWMutex

	peerClients map[string]*GRPCClient
}

func NewRaftServer(nodeId string, bindAddr string, grpcAddr string, httpAddr string, dataDir string, bootstrap bool, logger *zap.Logger) (*RaftServer, error) {
	fsmPath := filepath.Join(dataDir, "kvs")
	fsm, err := NewRaftFSM(fsmPath, logger)
	if err != nil {
		logger.Error("failed to create FSM", zap.String("path", fsmPath), zap.Error(err))
		return nil, err
	}

	return &RaftServer{
		nodeId:    nodeId,
		bindAddr:  bindAddr,
		grpcAddr:  grpcAddr,
		httpAddr:  httpAddr,
		dataDir:   dataDir,
		bootstrap: bootstrap,
		fsm:       fsm,
		logger:    logger,

		peerClients: make(map[string]*GRPCClient, 0),
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

	// create raft log store
	raftLogStorePath := filepath.Join(s.dataDir, "raft.db")
	raftLogStore, err := raftboltdb.NewBoltStore(raftLogStorePath)
	if err != nil {
		s.logger.Error("failed to create raft log store", zap.String("path", raftLogStorePath), zap.Error(err))
		return err
	}

	// create raft
	s.raft, err = raft.NewRaft(config, s.fsm, raftLogStore, raftLogStore, snapshotStore, s.transport)
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

		// wait for detect a leader
		timeout := 60 * time.Second
		err = s.WaitForDetectLeader(timeout)
		if err != nil {
			if err == ceteerrors.ErrTimeout {
				s.logger.Error("leader detection timed out", zap.Duration("timeout", timeout), zap.Error(err))
			} else {
				s.logger.Error("failed to detect leader", zap.Error(err))
			}
			return err
		}

		req := &pbkvs.JoinRequest{
			Id:       s.nodeId,
			BindAddr: s.bindAddr,
			GrpcAddr: s.grpcAddr,
			HttpAddr: s.httpAddr,
		}
		err = s.join(req)
		if err != nil {
			s.logger.Error("failed to join node to the cluster", zap.Any("req", req), zap.Error(err))
			return err
		}
	}

	//go func() {
	//	s.startUpdateNode(500 * time.Millisecond)
	//}()

	//go func() {
	//	s.startUpdateCluster(500 * time.Millisecond)
	//}()

	s.logger.Info("Raft server started", zap.String("addr", s.bindAddr))
	return nil
}

func (s *RaftServer) Stop() error {
	//s.stopUpdateNode()

	//s.stopUpdateCluster()

	if err := s.fsm.Close(); err != nil {
		s.logger.Error("failed to close FSM", zap.Error(err))
	}

	s.logger.Info("Raft server stopped", zap.String("addr", s.bindAddr))
	return nil
}

func (s *RaftServer) startUpdateNode(checkInterval time.Duration) {
	s.logger.Info("start to update node info")

	s.updateNodeStopCh = make(chan struct{})
	s.updateNodeDoneCh = make(chan struct{})

	defer func() {
		close(s.updateNodeDoneCh)
	}()

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	timeout := 60 * time.Second
	if err := s.WaitForDetectLeader(timeout); err != nil {
		if err == ceteerrors.ErrTimeout {
			s.logger.Error("leader detection timed out", zap.Duration("timeout", timeout), zap.Error(err))
		} else {
			s.logger.Error("failed to detect leader", zap.Error(err))
		}
	}

	for {
		select {
		case <-s.updateNodeStopCh:
			s.logger.Info("received a request to stop updating the node info")
			return
		case <-ticker.C:
			s.logger.Debug("tick")

			//// get nodes
			//nodes, err := s.fsm.Nodes()
			//if err != nil {
			//	s.logger.Printf("[ERR] %v", err)
			//}
			//
			//// update node state
			//if nodes[s.nodeId].State != s.raft.State().String() {
			//	nodes[s.nodeId].State = s.raft.State().String()
			//}
		}
	}
}

func (s *RaftServer) stopUpdateNode() {
	s.logger.Info("stop updating the node info")

	if s.updateNodeStopCh != nil {
		s.logger.Debug("send a request to stop updating the node info")
		close(s.updateNodeStopCh)
	}

	s.logger.Info("wait for the updating node info to stopped")
	<-s.updateNodeDoneCh
	s.logger.Info("the updating node info has stopped")
}

func (s *RaftServer) startUpdateCluster(checkInterval time.Duration) {
	s.logger.Info("start to update cluster info")

	s.updateClusterStopCh = make(chan struct{})
	s.updateClusterDoneCh = make(chan struct{})

	defer func() {
		close(s.updateClusterDoneCh)
	}()

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	timeout := 60 * time.Second
	if err := s.WaitForDetectLeader(timeout); err != nil {
		if err == ceteerrors.ErrTimeout {
			s.logger.Error("leader detection timed out", zap.Duration("timeout", timeout), zap.Error(err))
		} else {
			s.logger.Error("failed to detect leader", zap.Error(err))
		}
	}

	for {
		select {
		case <-s.updateClusterStopCh:
			s.logger.Info("received a request to stop updating a cluster")
			return
		case <-ticker.C:
			s.logger.Debug("tick")

			//s.updateClusterMutex.Lock()
			//
			//// get nodes in the cluster
			//nodes, err := s.fsm.Nodes()
			//if err != nil {
			//	s.logger.Printf("[ERR] %v", err)
			//}
			//
			//// clients
			//for id, node := range nodes {
			//	if client, exist := s.peerClients[id]; exist {
			//		s.logger.Printf("[INFO] %s %s %s", id, client.conn.Target(), node.GrpcAddr)
			//		if client.conn.Target() != node.GrpcAddr {
			//			// reconnect
			//			delete(s.peerClients, id)
			//			err = client.Close()
			//			if err != nil {
			//				s.logger.Printf("[ERR] %v", err)
			//			}
			//			newClient, err := NewGRPCClient(node.GrpcAddr)
			//			if err != nil {
			//				s.logger.Printf("[ERR] %v", err)
			//				continue
			//			}
			//			s.peerClients[id] = newClient
			//		}
			//	} else {
			//		// connect
			//		newClient, err := NewGRPCClient(node.GrpcAddr)
			//		if err != nil {
			//			s.logger.Printf("[ERR] %v", err)
			//			continue
			//		}
			//		s.peerClients[id] = newClient
			//	}
			//}
			//// close the connection to the node that left
			//for id, client := range s.peerClients {
			//	if _, exist := nodes[id]; !exist {
			//		delete(s.peerClients, id)
			//		err = client.Close()
			//		if err != nil {
			//			s.logger.Printf("[ERR] %v", err)
			//		}
			//	}
			//}
			//
			//// update node state
			//nodes[s.nodeId].State = s.raft.State().String()
			//
			//// nodes
			//for id, client := range s.peerClients {
			//	if resp, err := client.Node();  err != nil {
			//		s.logger.Printf("[ERR] %v", err)
			//		node := &pbkvs.Node{
			//			BindAddr: s.bindAddr,
			//			GrpcAddr: s.grpcAddr,
			//			State: raft.Shutdown.String(),
			//		}
			//		s.fsm.setNode(id, node)
			//	} else {
			//		s.fsm.setNode(id, resp.Node)
			//	}
			//}
			//
			//if resp, err := s.Cluster();err != nil {
			//	s.logger.Printf("[ERR] %v", err)
			//} else {
			//	s.logger.Printf("[DEBUG] %v", resp.Nodes)
			//}
			//
			//s.updateClusterMutex.Unlock()

			//// update node state
			//node := nodes[s.nodeId]
			//node.State = s.raft.State().String()

			//status, err := s.Cluster() // TODO: wait for cluster ready
			//if err != nil {
			//	s.logger.Printf("[ERR] %v", err)
			//}
			//s.logger.Printf("[INFO] %v", status)
			//default:
			//	// sleep
			//	time.Sleep(100 * time.Millisecond)
		}
	}
}

func (s *RaftServer) stopUpdateCluster() {
	s.logger.Info("stop to update cluster info")

	s.updateClusterMutex.Lock()
	for id, client := range s.peerClients {
		s.logger.Info("close peer client", zap.String("id", id), zap.String("addr", client.conn.Target()))
		err := client.Close()
		if err != nil {
			s.logger.Info("failed to close peer client", zap.String("id", id), zap.String("addr", client.conn.Target()), zap.Error(err))
		}
	}
	s.updateClusterMutex.Unlock()

	if s.updateClusterStopCh != nil {
		s.logger.Info("send a request to stop updating a cluster")
		close(s.updateClusterStopCh)
	}

	s.logger.Info("wait for the cluster update to stop")
	<-s.updateClusterDoneCh
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
			err := ceteerrors.ErrTimeout
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

	err = ceteerrors.ErrNotFoundLeader
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

func (s *RaftServer) join(req *pbkvs.JoinRequest) error {
	nodeAny := &any.Any{}
	err := protobuf.UnmarshalAny(req, nodeAny)
	if err != nil {
		s.logger.Error("failed to unmarshal request to the command data", zap.Any("req", req), zap.Error(err))
		return err
	}

	c := &pbkvs.KVSCommand{
		Type: pbkvs.KVSCommand_JOIN,
		Data: nodeAny,
	}

	msg, err := proto.Marshal(c)
	if err != nil {
		s.logger.Error("failed to marshal the command into the bytes as message", zap.Error(err))
		return err
	}

	f := s.raft.Apply(msg, 10*time.Second)
	if err = f.Error(); err != nil {
		s.logger.Error("failed to apply message", zap.Error(err))
		return err
	}

	return nil
}

func (s *RaftServer) Join(req *pbkvs.JoinRequest) error {
	cf := s.raft.GetConfiguration()
	err := cf.Error()
	if err != nil {
		s.logger.Error("failed to get Raft configuration", zap.Error(err))
		return err
	}

	for _, server := range cf.Configuration().Servers {
		if server.ID == raft.ServerID(req.Id) {
			s.logger.Info("node already joined the cluster", zap.String("id", req.Id))
			return nil
		}
	}

	f := s.raft.AddVoter(raft.ServerID(req.Id), raft.ServerAddress(req.BindAddr), 0, 0)
	err = f.Error()
	if err != nil {
		s.logger.Error("failed to add voter", zap.String("id", req.Id), zap.String("addr", req.BindAddr), zap.Error(err))
		return err
	}

	err = s.join(req)
	if err != nil {
		s.logger.Error("failed to join node", zap.Any("req", req), zap.Error(err))
		return nil
	}

	s.logger.Info("the node has successfully joined the cluster", zap.Any("req", req))
	return nil
}

func (s *RaftServer) leave(req *pbkvs.LeaveRequest) error {
	nodeAny := &any.Any{}
	err := protobuf.UnmarshalAny(req, nodeAny)
	if err != nil {
		s.logger.Error("failed to unmarshal request to the command data", zap.Any("req", req), zap.Error(err))
		return err
	}

	c := &pbkvs.KVSCommand{
		Type: pbkvs.KVSCommand_LEAVE,
		Data: nodeAny,
	}

	msg, err := proto.Marshal(c)
	if err != nil {
		s.logger.Error("failed to marshal the command into the bytes as the message", zap.Error(err))
		return err
	}

	f := s.raft.Apply(msg, 10*time.Second)
	if err = f.Error(); err != nil {
		s.logger.Error("failed to apply the message", zap.Error(err))
		return err
	}

	return nil
}

func (s *RaftServer) Leave(req *pbkvs.LeaveRequest) error {
	cf := s.raft.GetConfiguration()
	err := cf.Error()
	if err != nil {
		s.logger.Error("failed to get Raft configuration", zap.Error(err))
		return err
	}

	for _, server := range cf.Configuration().Servers {
		if server.ID == raft.ServerID(req.Id) {
			f := s.raft.RemoveServer(server.ID, 0, 0)
			if err = f.Error(); err != nil {
				s.logger.Error("failed to remove server", zap.String("id", req.Id), zap.Error(err))
				return err
			} else {
				s.logger.Info("the server has successfully removed", zap.String("id", req.Id))
				break
			}
		}
	}

	// delete metadata
	if err = s.leave(req); err != nil {
		s.logger.Error("failed to join node", zap.Any("req", req), zap.Error(err))
		return err
	}

	s.logger.Info("the node has successfully leaved from the cluster", zap.Any("req", req))
	return nil
}

func (s *RaftServer) Node() (*pbkvs.NodeResponse, error) {
	cf := s.raft.GetConfiguration()
	if err := cf.Error(); err != nil {
		s.logger.Error("failed to get Raft configuration", zap.Error(err))
		return nil, err
	}

	node := &pbkvs.Node{}
	for _, server := range cf.Configuration().Servers {
		if server.ID == raft.ServerID(s.nodeId) {
			node.BindAddr = string(server.Address)
			node.State = s.raft.State().String()
			if metadata := s.fsm.getMetadata(s.nodeId); metadata != nil {
				node.GrpcAddr = metadata.GrpcAddr
				node.HttpAddr = metadata.HttpAddr
			}
			break
		}
	}

	return &pbkvs.NodeResponse{
		Node: node,
	}, nil
}

func (s *RaftServer) Cluster() (*pbkvs.ClusterResponse, error) {
	cf := s.raft.GetConfiguration()
	if err := cf.Error(); err != nil {
		s.logger.Error("failed to get Raft configuration", zap.Error(err))
		return nil, err
	}

	nodes := make(map[string]*pbkvs.Node, 0)
	for _, server := range cf.Configuration().Servers {
		if server.ID == raft.ServerID(s.nodeId) {
			if resp, err := s.Node(); err != nil {
				s.logger.Error("failed to get node info", zap.Error(err))
				nodes[string(server.ID)] = resp.Node
			} else {
				nodes[string(server.ID)] = resp.Node
			}
		} else {
			node := &pbkvs.Node{}

			if metadata := s.fsm.getMetadata(string(server.ID)); metadata != nil {
				grpcAddr := metadata.GrpcAddr
				if client, err := NewGRPCClient(grpcAddr); err != nil {
					s.logger.Error("failed to create client", zap.String("addr", grpcAddr), zap.Error(err))
					node.State = raft.Shutdown.String()
				} else {
					if resp, err := client.Node(); err != nil {
						s.logger.Error("failed to get node info", zap.String("addr", grpcAddr), zap.Error(err))
						node.State = raft.Shutdown.String()
					} else {
						node = resp.Node
					}
					if err = client.Close(); err != nil {
						s.logger.Error("failed to close client", zap.String("addr", grpcAddr), zap.Error(err))
					}
				}
			} else {
				s.logger.Error("metadata not found", zap.String("id", string(server.ID)))
				node.State = raft.Shutdown.String()
			}

			nodes[string(server.ID)] = node
		}
	}

	return &pbkvs.ClusterResponse{
		Nodes: nodes,
	}, nil
}

func (s *RaftServer) Snapshot() error {
	if future := s.raft.Snapshot(); future.Error() != nil {
		s.logger.Error("failed to snapshot", zap.Error(future.Error()))
		return future.Error()
	}

	return nil
}

func (s *RaftServer) Get(req *pbkvs.GetRequest) (*pbkvs.GetResponse, error) {
	value, err := s.fsm.Get(req.Key)
	if err != nil {
		s.logger.Error("failed to get", zap.Any("key", req.Key), zap.Error(err))
		return nil, err
	}

	resp := &pbkvs.GetResponse{
		Value: value,
	}

	return resp, nil
}

func (s *RaftServer) Set(req *pbkvs.PutRequest) error {
	kvpAny := &any.Any{}
	if err := protobuf.UnmarshalAny(req, kvpAny); err != nil {
		s.logger.Error("failed to unmarshal request to the command data", zap.Binary("key", req.Key), zap.Error(err))
		return err
	}

	c := &pbkvs.KVSCommand{
		Type: pbkvs.KVSCommand_PUT,
		Data: kvpAny,
	}

	msg, err := proto.Marshal(c)
	if err != nil {
		s.logger.Error("failed to marshal the command into the bytes as the message", zap.Binary("key", req.Key), zap.Error(err))
		return err
	}

	if future := s.raft.Apply(msg, 10*time.Second); future.Error() != nil {
		s.logger.Error("failed to apply the message", zap.Error(future.Error()))
		return future.Error()
	}

	return nil
}

func (s *RaftServer) Delete(req *pbkvs.DeleteRequest) error {
	kvpAny := &any.Any{}
	if err := protobuf.UnmarshalAny(req, kvpAny); err != nil {
		s.logger.Error("failed to unmarshal request to the command data", zap.Binary("key", req.Key), zap.Error(err))
		return err
	}

	c := &pbkvs.KVSCommand{
		Type: pbkvs.KVSCommand_DELETE,
		Data: kvpAny,
	}

	msg, err := proto.Marshal(c)
	if err != nil {
		s.logger.Error("failed to marshal the command into the bytes as the message", zap.Binary("key", req.Key), zap.Error(err))
		return err
	}

	if future := s.raft.Apply(msg, 10*time.Second); future.Error() != nil {
		s.logger.Error("failed to unmarshal request to the command data", zap.Binary("key", req.Key), zap.Error(future.Error()))
		return future.Error()
	}

	return nil
}
