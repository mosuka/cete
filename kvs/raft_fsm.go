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
	"errors"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/mosuka/cete/protobuf"
	pbkvs "github.com/mosuka/cete/protobuf/kvs"
	"go.uber.org/zap"
)

type RaftFSM struct {
	logger *zap.Logger

	kvs        *KVS
	metadata   map[string]*Metadata
	nodesMutex sync.RWMutex
}

func NewRaftFSM(path string, logger *zap.Logger) (*RaftFSM, error) {
	err := os.MkdirAll(path, 0755)
	if err != nil && !os.IsExist(err) {
		logger.Error("failed to make directories", zap.String("path", path), zap.Error(err))
		return nil, err
	}

	kvs, err := NewKVS(path, path, logger)
	if err != nil {
		logger.Error("failed to create key value store", zap.String("path", path), zap.Error(err))
		return nil, err
	}

	return &RaftFSM{
		logger:   logger,
		kvs:      kvs,
		metadata: make(map[string]*Metadata, 0),
	}, nil
}

func (f *RaftFSM) Close() error {
	err := f.kvs.Close()
	if err != nil {
		f.logger.Error("failed to close key value store", zap.Error(err))
		return err
	}

	return nil
}

func (f *RaftFSM) Get(key string) ([]byte, error) {
	value, err := f.kvs.Get(key)
	if err != nil {
		f.logger.Error("failed to get value", zap.String("key", key), zap.Error(err))
		return nil, err
	}

	return value, nil
}

func (f *RaftFSM) applySet(key string, value []byte) interface{} {
	err := f.kvs.Set(key, value)
	if err != nil {
		f.logger.Error("failed to set value", zap.String("key", key), zap.Error(err))
		return err
	}

	return nil
}

func (f *RaftFSM) applyDelete(key string) interface{} {
	err := f.kvs.Delete(key)
	if err != nil {
		f.logger.Error("failed to delete value", zap.String("key", key), zap.Error(err))
		return err
	}

	return nil
}

func (f *RaftFSM) getMetadata(id string) *Metadata {
	if metadata, exists := f.metadata[id]; exists {
		return metadata
	} else {
		f.logger.Warn("metadata not found", zap.String("id", id))
		return nil
	}
}

func (f *RaftFSM) setMetadata(id string, metadata *Metadata) {
	f.nodesMutex.Lock()
	f.metadata[id] = metadata
	f.nodesMutex.Unlock()
}

func (f *RaftFSM) deleteMetadata(id string) {
	f.nodesMutex.Lock()
	if _, exists := f.metadata[id]; exists {
		delete(f.metadata, id)
	} else {
		f.logger.Warn("metadata not found", zap.String("id", id))
	}
	f.nodesMutex.Unlock()
}

func (f *RaftFSM) applyJoin(nodeId string, grpcAddr string, httpAddr string) interface{} {
	f.setMetadata(nodeId, &Metadata{GrpcAddr: grpcAddr, HttpAddr: httpAddr})

	return nil
}

func (f *RaftFSM) applyLeave(nodeId string) interface{} {
	f.deleteMetadata(nodeId)

	return nil
}

func (f *RaftFSM) Apply(l *raft.Log) interface{} {
	var c pbkvs.KVSCommand
	err := proto.Unmarshal(l.Data, &c)
	if err != nil {
		f.logger.Error("failed to unmarshal message bytes to KVS command", zap.Error(err))
		return err
	}

	switch c.Type {
	case pbkvs.KVSCommand_JOIN:
		joinRequestInstance, err := protobuf.MarshalAny(c.Data)
		if err != nil {
			f.logger.Error("failed to marshal to request from KVS command request", zap.String("type", c.Type.String()), zap.Error(err))
			return err
		}
		if joinRequestInstance == nil {
			err = errors.New("nil")
			f.logger.Error("request is nil", zap.String("type", c.Type.String()), zap.Error(err))
			return err
		}
		joinRequest := joinRequestInstance.(*pbkvs.JoinRequest)

		return f.applyJoin(joinRequest.Id, joinRequest.GrpcAddr, joinRequest.HttpAddr)
	case pbkvs.KVSCommand_LEAVE:
		leaveRequestInstance, err := protobuf.MarshalAny(c.Data)
		if err != nil {
			f.logger.Error("failed to marshal to request from KVS command request", zap.String("type", c.Type.String()), zap.Error(err))
			return err
		}
		if leaveRequestInstance == nil {
			err = errors.New("nil")
			f.logger.Error("request is nil", zap.String("type", c.Type.String()), zap.Error(err))
			return err
		}
		leaveRequest := *leaveRequestInstance.(*pbkvs.LeaveRequest)

		return f.applyLeave(leaveRequest.Id)
	case pbkvs.KVSCommand_PUT:
		putRequestInstance, err := protobuf.MarshalAny(c.Data)
		if err != nil {
			f.logger.Error("failed to marshal to request from KVS command request", zap.String("type", c.Type.String()), zap.Error(err))
			return err
		}
		if putRequestInstance == nil {
			err = errors.New("nil")
			f.logger.Error("request is nil", zap.String("type", c.Type.String()), zap.Error(err))
			return err
		}
		putRequest := *putRequestInstance.(*pbkvs.PutRequest)

		return f.applySet(putRequest.Key, putRequest.Value)
	case pbkvs.KVSCommand_DELETE:
		deleteRequestInstance, err := protobuf.MarshalAny(c.Data)
		if err != nil {
			f.logger.Error("failed to marshal to request from KVS command request", zap.String("type", c.Type.String()), zap.Error(err))
			return err
		}
		if deleteRequestInstance == nil {
			err = errors.New("nil")
			f.logger.Error("request is nil", zap.String("type", c.Type.String()), zap.Error(err))
			return err
		}
		deleteRequest := *deleteRequestInstance.(*pbkvs.DeleteRequest)

		return f.applyDelete(deleteRequest.Key)
	default:
		err = errors.New("command type not support")
		f.logger.Error("unsupported command", zap.String("type", c.Type.String()), zap.Error(err))
		return err
	}
}

func (f *RaftFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &KVSFSMSnapshot{
		kvs:    f.kvs,
		logger: f.logger,
	}, nil
}

func (f *RaftFSM) Restore(rc io.ReadCloser) error {
	start := time.Now()

	f.logger.Info("start to restore items")

	defer func() {
		err := rc.Close()
		if err != nil {
			f.logger.Error("failed to close reader", zap.Error(err))
		}
	}()

	data, err := ioutil.ReadAll(rc)
	if err != nil {
		f.logger.Error("failed to open reader", zap.Error(err))
		return err
	}

	keyCount := uint64(0)

	buff := proto.NewBuffer(data)
	for {
		kvp := &pbkvs.KeyValuePair{}
		err = buff.DecodeMessage(kvp)
		if err == io.ErrUnexpectedEOF {
			f.logger.Debug("reached the EOF", zap.Error(err))
			break
		}
		if err != nil {
			f.logger.Error("failed to read key value pair", zap.Error(err))
			return err
		}

		// apply item to store
		err = f.kvs.Set(kvp.Key, kvp.Value)
		if err != nil {
			f.logger.Error("failed to set key value pair to key value store", zap.Error(err))
			return err
		}

		f.logger.Debug("restore", zap.String("key", kvp.Key))
		keyCount = keyCount + 1
	}

	f.logger.Info("finished to restore items", zap.Uint64("count", keyCount), zap.Float64("time", float64(time.Since(start))/float64(time.Second)))

	return nil
}

// ---------------------

type KVSFSMSnapshot struct {
	kvs    *KVS
	logger *zap.Logger
}

func (f *KVSFSMSnapshot) Persist(sink raft.SnapshotSink) error {
	start := time.Now()

	f.logger.Info("start to persist items")

	defer func() {
		err := sink.Close()
		if err != nil {
			f.logger.Error("failed to close sink", zap.Error(err))
		}
	}()

	ch := f.kvs.SnapshotItems()

	kvpCount := uint64(0)

	for {
		kvp := <-ch
		if kvp == nil {
			f.logger.Debug("channel closed")
			break
		}

		kvpCount = kvpCount + 1

		buff := proto.NewBuffer([]byte{})
		err := buff.EncodeMessage(kvp)
		if err != nil {
			f.logger.Error("failed to encode key value pair", zap.Error(err))
			return err
		}

		_, err = sink.Write(buff.Bytes())
		if err != nil {
			f.logger.Error("failed to write key value pair", zap.Error(err))
			return err
		}
	}

	f.logger.Info("finished to persist items", zap.Uint64("count", kvpCount), zap.Float64("time", float64(time.Since(start))/float64(time.Second)))

	return nil
}

func (f *KVSFSMSnapshot) Release() {
	f.logger.Info("release")
}
