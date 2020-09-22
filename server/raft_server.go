package server

import (
	"encoding/json"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	raftbadgerdb "github.com/BBVA/raft-badger"
	"github.com/dgraph-io/badger/v2"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/hashicorp/raft"
	"github.com/mosuka/cete/errors"
	"github.com/mosuka/cete/marshaler"
	"github.com/mosuka/cete/metric"
	"github.com/mosuka/cete/protobuf"
	"go.uber.org/zap"
)

type RaftServer struct {
	id            string
	raftAddress   string
	dataDirectory string
	bootstrap     bool
	logger        *zap.Logger

	fsm *RaftFSM

	transport *raft.NetworkTransport
	raft      *raft.Raft

	watchClusterStopCh chan struct{}
	watchClusterDoneCh chan struct{}

	applyCh chan *protobuf.Event
}

func NewRaftServer(id string, raftAddress string, dataDirectory string, bootstrap bool, logger *zap.Logger) (*RaftServer, error) {
	fsmPath := filepath.Join(dataDirectory, "kvs")
	fsm, err := NewRaftFSM(fsmPath, logger)
	if err != nil {
		logger.Error("failed to create FSM", zap.String("path", fsmPath), zap.Error(err))
		return nil, err
	}

	return &RaftServer{
		id:            id,
		raftAddress:   raftAddress,
		dataDirectory: dataDirectory,
		bootstrap:     bootstrap,
		fsm:           fsm,
		logger:        logger,

		watchClusterStopCh: make(chan struct{}),
		watchClusterDoneCh: make(chan struct{}),

		applyCh: make(chan *protobuf.Event, 1024),
	}, nil
}

func (s *RaftServer) Start() error {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(s.id)
	config.SnapshotThreshold = 1024
	config.LogOutput = ioutil.Discard

	addr, err := net.ResolveTCPAddr("tcp", s.raftAddress)
	if err != nil {
		s.logger.Error("failed to resolve TCP address", zap.String("raft_address", s.raftAddress), zap.Error(err))
		return err
	}

	s.transport, err = raft.NewTCPTransport(s.raftAddress, addr, 3, 10*time.Second, ioutil.Discard)
	if err != nil {
		s.logger.Error("failed to create TCP transport", zap.String("raft_address", s.raftAddress), zap.Error(err))
		return err
	}

	// create snapshot store
	snapshotStore, err := raft.NewFileSnapshotStore(s.dataDirectory, 2, ioutil.Discard)
	if err != nil {
		s.logger.Error("failed to create file snapshot store", zap.String("path", s.dataDirectory), zap.Error(err))
		return err
	}

	logStorePath := filepath.Join(s.dataDirectory, "raft", "log")
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

	stableStorePath := filepath.Join(s.dataDirectory, "raft", "stable")
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

	s.logger.Info("Raft server started", zap.String("raft_address", s.raftAddress))
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
	s.logger.Info("Raft has shutdown", zap.String("raft_address", s.raftAddress))

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
		case <-ticker.C:
			raftStats := s.raft.Stats()

			switch raftStats["state"] {
			case "Follower":
				metric.RaftStateMetric.WithLabelValues(s.id).Set(float64(raft.Follower))
			case "Candidate":
				metric.RaftStateMetric.WithLabelValues(s.id).Set(float64(raft.Candidate))
			case "Leader":
				metric.RaftStateMetric.WithLabelValues(s.id).Set(float64(raft.Leader))
			case "Shutdown":
				metric.RaftStateMetric.WithLabelValues(s.id).Set(float64(raft.Shutdown))
			}

			if term, err := strconv.ParseFloat(raftStats["term"], 64); err == nil {
				metric.RaftTermMetric.WithLabelValues(s.id).Set(term)
			}

			if lastLogIndex, err := strconv.ParseFloat(raftStats["last_log_index"], 64); err == nil {
				metric.RaftLastLogIndexMetric.WithLabelValues(s.id).Set(lastLogIndex)
			}

			if lastLogTerm, err := strconv.ParseFloat(raftStats["last_log_term"], 64); err == nil {
				metric.RaftLastLogTermMetric.WithLabelValues(s.id).Set(lastLogTerm)
			}

			if commitIndex, err := strconv.ParseFloat(raftStats["commit_index"], 64); err == nil {
				metric.RaftCommitIndexMetric.WithLabelValues(s.id).Set(commitIndex)
			}

			if appliedIndex, err := strconv.ParseFloat(raftStats["applied_index"], 64); err == nil {
				metric.RaftAppliedIndexMetric.WithLabelValues(s.id).Set(appliedIndex)
			}

			if fsmPending, err := strconv.ParseFloat(raftStats["fsm_pending"], 64); err == nil {
				metric.RaftFsmPendingMetric.WithLabelValues(s.id).Set(fsmPending)
			}

			if lastSnapshotIndex, err := strconv.ParseFloat(raftStats["last_snapshot_index"], 64); err == nil {
				metric.RaftLastSnapshotIndexMetric.WithLabelValues(s.id).Set(lastSnapshotIndex)
			}

			if lastSnapshotTerm, err := strconv.ParseFloat(raftStats["last_snapshot_term"], 64); err == nil {
				metric.RaftLastSnapshotTermMetric.WithLabelValues(s.id).Set(lastSnapshotTerm)
			}

			if latestConfigurationIndex, err := strconv.ParseFloat(raftStats["latest_configuration_index"], 64); err == nil {
				metric.RaftLatestConfigurationIndexMetric.WithLabelValues(s.id).Set(latestConfigurationIndex)
			}

			if numPeers, err := strconv.ParseFloat(raftStats["num_peers"], 64); err == nil {
				metric.RaftNumPeersMetric.WithLabelValues(s.id).Set(numPeers)
			}

			if lastContact, err := strconv.ParseFloat(raftStats["last_contact"], 64); err == nil {
				metric.RaftLastContactMetric.WithLabelValues(s.id).Set(lastContact)
			}

			if nodes, err := s.Nodes(); err == nil {
				metric.RaftNumNodesMetric.WithLabelValues(s.id).Set(float64(len(nodes)))
			}

			kvsStats := s.fsm.Stats()

			if numReads, err := strconv.ParseFloat(kvsStats["num_reads"], 64); err == nil {
				metric.KvsNumReadsMetric.WithLabelValues(s.id).Set(numReads)
			}

			if numWrites, err := strconv.ParseFloat(kvsStats["num_writes"], 64); err == nil {
				metric.KvsNumWritesMetric.WithLabelValues(s.id).Set(numWrites)
			}

			if numBytesRead, err := strconv.ParseFloat(kvsStats["num_bytes_read"], 64); err == nil {
				metric.KvsNumBytesReadMetric.WithLabelValues(s.id).Set(numBytesRead)
			}

			if numBytesWritten, err := strconv.ParseFloat(kvsStats["num_bytes_written"], 64); err == nil {
				metric.KvsNumBytesWrittenMetric.WithLabelValues(s.id).Set(numBytesWritten)
			}

			var numLsmGets map[string]interface{}
			if err := json.Unmarshal([]byte(kvsStats["num_lsm_gets"]), &numLsmGets); err == nil {
				for key, value := range numLsmGets {
					s.logger.Info("", zap.String("key", key), zap.Any("value", value))
				}
			}

			var numLsmBloomHits map[string]interface{}
			if err := json.Unmarshal([]byte(kvsStats["num_lsm_bloom_Hits"]), &numLsmBloomHits); err == nil {
				for key, value := range numLsmBloomHits {
					s.logger.Info("", zap.String("key", key), zap.Any("value", value))
				}
			}

			if numGets, err := strconv.ParseFloat(kvsStats["num_gets"], 64); err == nil {
				metric.KvsNumGetsMetric.WithLabelValues(s.id).Set(numGets)
			}

			if numPuts, err := strconv.ParseFloat(kvsStats["num_puts"], 64); err == nil {
				metric.KvsNumPutsMetric.WithLabelValues(s.id).Set(numPuts)
			}

			if numBlockedPuts, err := strconv.ParseFloat(kvsStats["num_blocked_puts"], 64); err == nil {
				metric.KvsNumBlockedPutsMetric.WithLabelValues(s.id).Set(numBlockedPuts)
			}

			if numMemtablesGets, err := strconv.ParseFloat(kvsStats["num_memtables_gets"], 64); err == nil {
				metric.KvsNumMemtablesGetsMetric.WithLabelValues(s.id).Set(numMemtablesGets)
			}

			var lsmSize map[string]interface{}
			if err := json.Unmarshal([]byte(kvsStats["lsm_size"]), &lsmSize); err == nil {
				for key, value := range lsmSize {
					metric.KvsLSMSizeMetric.WithLabelValues(s.id, key).Set(value.(float64))
				}
			}

			var vlogSize map[string]interface{}
			if err := json.Unmarshal([]byte(kvsStats["vlog_size"]), &vlogSize); err == nil {
				for key, value := range vlogSize {
					metric.KvsVlogSizeMetric.WithLabelValues(s.id, key).Set(value.(float64))
				}
			}

			var pendingWrites map[string]interface{}
			if err := json.Unmarshal([]byte(kvsStats["pending_writes"]), &pendingWrites); err == nil {
				for key, value := range pendingWrites {
					metric.KvsPendingWritesMetric.WithLabelValues(s.id, key).Set(value.(float64))
				}
			}
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
				s.logger.Debug("detected a leader address", zap.String("raft_address", string(leaderAddr)))
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
	leaderAddr, err := s.LeaderAddress(timeout)
	if err != nil {
		s.logger.Error("failed to get leader address", zap.Error(err))
		return "", err
	}

	cf := s.raft.GetConfiguration()
	if err := cf.Error(); err != nil {
		s.logger.Error("failed to get Raft configuration", zap.Error(err))
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

func (s *RaftServer) State() raft.RaftState {
	return s.raft.State()
}

func (s *RaftServer) StateStr() string {
	return s.State().String()
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
		s.logger.Debug("node already exists", zap.String("id", id), zap.String("raft_address", node.RaftAddress))
	} else {
		if future := s.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(node.RaftAddress), 0, 0); future.Error() != nil {
			s.logger.Error("failed to add voter", zap.String("id", id), zap.String("raft_address", node.RaftAddress), zap.Error(future.Error()))
			return future.Error()
		}
		s.logger.Info("node has successfully joined", zap.String("id", id), zap.String("raft_address", node.RaftAddress))
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

	node, ok := nodes[s.id]
	if !ok {
		return nil, errors.ErrNotFound
	}

	node.State = s.StateStr()

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
			RaftAddress: string(server.Address),
			Metadata:    s.fsm.getMetadata(string(server.ID)),
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

func (s *RaftServer) Scan(req *protobuf.ScanRequest) (*protobuf.ScanResponse, error) {
	values, err := s.fsm.Scan(req.Prefix)
	if err != nil {
		s.logger.Error("failed to scan", zap.Any("prefix", req.Prefix), zap.Error(err))
		return nil, err
	}

	resp := &protobuf.ScanResponse{
		Values: values,
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
