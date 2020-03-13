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
	"net"
	"path/filepath"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/mosuka/cete/errors"
	"github.com/mosuka/cete/protobuf"
	pbkvs "github.com/mosuka/cete/protobuf/kvs"
)

type RaftServer struct {
	nodeId    string
	bindAddr  string
	grpcAddr  string
	httpAddr  string
	dataDir   string
	bootstrap bool
	logger    *log.Logger

	fsm *RaftFSM

	//transport *raftgrpc.RaftGRPCTransport
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

func NewRaftServer(nodeId string, bindAddr string, grpcAddr string, httpAddr string, dataDir string, bootstrap bool, logger *log.Logger) (*RaftServer, error) {
	fsm, err := NewRaftFSM(filepath.Join(dataDir, "kvs"), logger)
	if err != nil {
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
	config.Logger = s.logger

	addr, err := net.ResolveTCPAddr("tcp", s.bindAddr)
	if err != nil {
		return err
	}

	//s.transport = raftgrpc.NewTransport(context.Background(), string(config.LocalID))
	//s.transport = raftgrpc.NewTransport(context.TODO(), s.nodeId)
	s.transport, err = raft.NewTCPTransportWithLogger(s.bindAddr, addr, 3, 10*time.Second, s.logger)
	if err != nil {
		return err
	}

	// create snapshot store
	snapshotStore, err := raft.NewFileSnapshotStoreWithLogger(s.dataDir, 2, s.logger)
	if err != nil {
		return err
	}

	// create raft log store
	raftLogStore, err := raftboltdb.NewBoltStore(filepath.Join(s.dataDir, "raft.db"))
	if err != nil {
		return err
	}

	// create raft
	s.raft, err = raft.NewRaft(config, s.fsm, raftLogStore, raftLogStore, snapshotStore, s.transport)
	if err != nil {
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
		err = s.WaitForDetectLeader(60 * time.Second)
		if err != nil {
			if err == errors.ErrTimeout {
				s.logger.Printf("[WARN] %v", err)
			} else {
				s.logger.Printf("[ERR] %v", err)
				return nil
			}
		}

		err = s.join(
			&pbkvs.JoinRequest{
				Id:       s.nodeId,
				BindAddr: s.bindAddr,
				GrpcAddr: s.grpcAddr,
				HttpAddr: s.httpAddr,
			},
		)
		if err != nil {
			s.logger.Printf("[ERR] %v", err)
			return nil
		}
	}

	//go func() {
	//	s.startUpdateNode(500 * time.Millisecond)
	//}()

	//go func() {
	//	s.startUpdateCluster(500 * time.Millisecond)
	//}()

	return nil
}

func (s *RaftServer) Stop() error {
	//s.stopUpdateNode()

	//s.stopUpdateCluster()

	err := s.fsm.Close()
	if err != nil {
		return err
	}

	return nil
}

func (s *RaftServer) startUpdateNode(checkInterval time.Duration) {
	s.logger.Printf("[INFO] start to update node info")

	s.updateNodeStopCh = make(chan struct{})
	s.updateNodeDoneCh = make(chan struct{})

	defer func() {
		close(s.updateNodeDoneCh)
	}()

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	s.WaitForDetectLeader(60 * time.Second)

	for {
		select {
		case <-s.updateNodeStopCh:
			s.logger.Printf("[INFO] received a request to stop updating the node info")
			return
		case <-ticker.C:
			s.logger.Printf("[DEBUG] tick")

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
	s.logger.Printf("[INFO] stop updating the node info")

	if s.updateNodeStopCh != nil {
		s.logger.Printf("[INFO] send a request to stop updating the node info")
		close(s.updateNodeStopCh)
	}

	s.logger.Printf("[INFO] wait for the updating node info to stopped")
	<-s.updateNodeDoneCh
	s.logger.Printf("[INFO] the updating node info has stopped")
}

func (s *RaftServer) startUpdateCluster(checkInterval time.Duration) {
	s.logger.Printf("[INFO] start to update cluster info")

	s.updateClusterStopCh = make(chan struct{})
	s.updateClusterDoneCh = make(chan struct{})

	defer func() {
		close(s.updateClusterDoneCh)
	}()

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	s.WaitForDetectLeader(60 * time.Second)

	for {
		select {
		case <-s.updateClusterStopCh:
			s.logger.Printf("[INFO] received a request to stop updating a cluster")
			return
		case <-ticker.C:
			s.logger.Printf("[DEBUG] tick")

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
	s.logger.Printf("[INFO] stop to update cluster info")

	s.updateClusterMutex.Lock()
	for id, client := range s.peerClients {
		s.logger.Printf("[INFO] close peer client %s %s", id, client.conn.Target())
		err := client.Close()
		if err != nil {
			s.logger.Printf("[ERR] %v", err)
		}
	}
	s.updateClusterMutex.Unlock()

	if s.updateClusterStopCh != nil {
		s.logger.Printf("[INFO] send a request to stop updating a cluster")
		close(s.updateClusterStopCh)
	}

	s.logger.Printf("[INFO] wait for the cluster update to stop")
	<-s.updateClusterDoneCh
	s.logger.Printf("[INFO] the cluster update has been stopped")
}

func (s *RaftServer) WaitForDetectLeader(timeout time.Duration) error {
	ticker := time.NewTicker(1000 * time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-ticker.C:
			leaderAddr := s.raft.Leader()
			if leaderAddr != "" {
				s.logger.Printf("[INFO] detected %v as a leader", leaderAddr)
				return nil
			} else {
				s.logger.Printf("[WARN] %v", errors.ErrNotFoundLeader)
			}
		case <-timer.C:
			return errors.ErrTimeout
		}
	}
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
				return leaderAddr, nil
			}
		case <-timer.C:
			return "", errors.ErrTimeout
		}
	}
}

func (s *RaftServer) LeaderID(timeout time.Duration) (raft.ServerID, error) {
	cf := s.raft.GetConfiguration()
	err := cf.Error()
	if err != nil {
		return "", err
	}

	leaderAddr, err := s.LeaderAddress(timeout)
	if err != nil {
		return "", err
	}

	for _, server := range cf.Configuration().Servers {
		if server.Address == leaderAddr {
			return server.ID, nil
		}
	}

	return "", errors.ErrNotFoundLeader
}

func (s *RaftServer) State() string {
	return s.raft.State().String()
}

func (s *RaftServer) join(req *pbkvs.JoinRequest) error {
	// Node -> Any
	nodeAny := &any.Any{}
	err := protobuf.UnmarshalAny(req, nodeAny)
	if err != nil {
		return err
	}

	c := &pbkvs.KVSCommand{
		Type: pbkvs.KVSCommand_JOIN,
		Data: nodeAny,
	}

	msg, err := proto.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(msg, 10*time.Second)
	err = f.Error()
	if err != nil {
		return err
	}

	return nil
}

func (s *RaftServer) Join(req *pbkvs.JoinRequest) error {
	cf := s.raft.GetConfiguration()
	err := cf.Error()
	if err != nil {
		return err
	}

	for _, server := range cf.Configuration().Servers {
		if server.ID == raft.ServerID(req.Id) {
			s.logger.Printf("[INFO] node %s already joined the cluster", req.Id)
			return nil
		}
	}

	f := s.raft.AddVoter(raft.ServerID(req.Id), raft.ServerAddress(req.BindAddr), 0, 0)
	err = f.Error()
	if err != nil {
		return err
	}

	err = s.join(req)
	if err != nil {
		s.logger.Printf("[ERR] %v", err)
		return nil
	}

	s.logger.Printf("[INFO] node %s at %s joined successfully", req.Id, req.GrpcAddr)
	return nil
}

func (s *RaftServer) leave(req *pbkvs.LeaveRequest) error {
	// Node -> Any
	nodeAny := &any.Any{}
	err := protobuf.UnmarshalAny(req, nodeAny)
	if err != nil {
		return err
	}

	c := &pbkvs.KVSCommand{
		Type: pbkvs.KVSCommand_LEAVE,
		Data: nodeAny,
	}

	msg, err := proto.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(msg, 10*time.Second)
	err = f.Error()
	if err != nil {
		return err
	}

	return nil
}

func (s *RaftServer) Leave(req *pbkvs.LeaveRequest) error {
	cf := s.raft.GetConfiguration()
	err := cf.Error()
	if err != nil {
		return err
	}

	for _, server := range cf.Configuration().Servers {
		if server.ID == raft.ServerID(req.Id) {
			f := s.raft.RemoveServer(server.ID, 0, 0)
			err = f.Error()
			if err != nil {
				return err
			}

			s.logger.Printf("[INFO] node %s leaved successfully", req.Id)
			return nil
		}
	}

	// delete metadata
	err = s.leave(req)
	if err != nil {
		s.logger.Printf("[ERR] %v", err)
		return nil
	}

	s.logger.Printf("[INFO] node %s does not exists in the cluster", req.Id)
	return nil
}

func (s *RaftServer) Node() (*pbkvs.NodeResponse, error) {
	cf := s.raft.GetConfiguration()
	if err := cf.Error(); err != nil {
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
		return nil, err
	}

	nodes := make(map[string]*pbkvs.Node, 0)
	for _, server := range cf.Configuration().Servers {
		if server.ID == raft.ServerID(s.nodeId) {
			if resp, err := s.Node(); err != nil {
				s.logger.Printf("[ERR] %v", err)
				nodes[string(server.ID)] = resp.Node
			} else {
				nodes[string(server.ID)] = resp.Node
			}
		} else {
			node := &pbkvs.Node{}

			if metadata := s.fsm.getMetadata(string(server.ID)); metadata != nil {
				grpcAddr := metadata.GrpcAddr
				if client, err := NewGRPCClient(grpcAddr); err != nil {
					s.logger.Printf("[ERR] %v", err)
					node.State = raft.Shutdown.String()
				} else {
					if resp, err := client.Node(); err != nil {
						s.logger.Printf("[ERR] %v", err)
						node.State = raft.Shutdown.String()
					} else {
						node = resp.Node
					}
					if err = client.Close(); err != nil {
						s.logger.Printf("[ERR] %v", err)
					}
				}
			} else {
				s.logger.Printf("[ERR] metadata not found")
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
	f := s.raft.Snapshot()
	err := f.Error()
	if err != nil {
		return err
	}

	return nil
}

func (s *RaftServer) Get(req *pbkvs.GetRequest) (*pbkvs.GetResponse, error) {
	value, err := s.fsm.Get(req.Key)
	if err != nil {
		return nil, err
	}

	resp := &pbkvs.GetResponse{
		Value: value,
	}

	return resp, nil
}

func (s *RaftServer) Set(kvp *pbkvs.PutRequest) error {
	kvpAny := &any.Any{}
	err := protobuf.UnmarshalAny(kvp, kvpAny)
	if err != nil {
		return err
	}

	c := &pbkvs.KVSCommand{
		Type: pbkvs.KVSCommand_PUT,
		Data: kvpAny,
	}

	msg, err := proto.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(msg, 10*time.Second)
	err = f.Error()
	if err != nil {
		return err
	}

	return nil
}

func (s *RaftServer) Delete(kvp *pbkvs.DeleteRequest) error {
	// KeyValuePair -> Any
	kvpAny := &any.Any{}
	err := protobuf.UnmarshalAny(kvp, kvpAny)
	if err != nil {
		return err
	}

	c := &pbkvs.KVSCommand{
		Type: pbkvs.KVSCommand_DELETE,
		Data: kvpAny,
	}

	msg, err := proto.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(msg, 10*time.Second)
	err = f.Error()
	if err != nil {
		return err
	}

	return nil
}
