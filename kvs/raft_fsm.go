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
	"errors"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/mosuka/cete/protobuf"
	pbkvs "github.com/mosuka/cete/protobuf/kvs"
)

type RaftFSM struct {
	logger *log.Logger

	kvs        *KVS
	metadata   map[string]*Metadata
	nodesMutex sync.RWMutex
}

func NewRaftFSM(path string, logger *log.Logger) (*RaftFSM, error) {
	err := os.MkdirAll(path, 0755)
	if err != nil && !os.IsExist(err) {
		return nil, err
	}

	kvs, err := NewKVS(path, path, logger)
	if err != nil {
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
		return err
	}

	return nil
}

func (f *RaftFSM) Get(key []byte) ([]byte, error) {
	value, err := f.kvs.Get(key)
	if err != nil {
		return nil, err
	}

	return value, nil
}

func (f *RaftFSM) applySet(key []byte, value []byte) interface{} {
	err := f.kvs.Set(key, value)
	if err != nil {
		f.logger.Printf("[ERR] %v", err)
		return err
	}

	return nil
}

func (f *RaftFSM) applyDelete(key []byte) interface{} {
	err := f.kvs.Delete(key)
	if err != nil {
		f.logger.Printf("[ERR] %v", err)
		return err
	}

	return nil
}

func (f *RaftFSM) getMetadata(nodeId string) *Metadata {
	if metadata, exists := f.metadata[nodeId]; exists {
		return metadata
	} else {
		return nil
	}
}

func (f *RaftFSM) setMetadata(nodeId string, metadata *Metadata) {
	f.nodesMutex.Lock()
	f.metadata[nodeId] = metadata
	f.nodesMutex.Unlock()
}

func (f *RaftFSM) deleteMetadata(nodeId string) {
	f.nodesMutex.Lock()
	if _, exists := f.metadata[nodeId]; exists {
		delete(f.metadata, nodeId)
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
		return err
	}

	switch c.Type {
	case pbkvs.KVSCommand_JOIN:
		joinRequestInstance, err := protobuf.MarshalAny(c.Data)
		if err != nil {
			return err
		}
		if joinRequestInstance == nil {
			return errors.New("nil")
		}
		joinRequest := joinRequestInstance.(*pbkvs.JoinRequest)

		return f.applyJoin(joinRequest.Id, joinRequest.GrpcAddr, joinRequest.HttpAddr)
	case pbkvs.KVSCommand_LEAVE:
		leaveRequestInstance, err := protobuf.MarshalAny(c.Data)
		if err != nil {
			return err
		}
		if leaveRequestInstance == nil {
			return errors.New("nil")
		}
		leaveRequest := *leaveRequestInstance.(*pbkvs.LeaveRequest)

		return f.applyLeave(leaveRequest.Id)
	case pbkvs.KVSCommand_PUT:
		putRequestInstance, err := protobuf.MarshalAny(c.Data)
		if err != nil {
			return err
		}
		if putRequestInstance == nil {
			return errors.New("nil")
		}
		putRequest := *putRequestInstance.(*pbkvs.PutRequest)

		return f.applySet(putRequest.Key, putRequest.Value)
	case pbkvs.KVSCommand_DELETE:
		deleteRequestInstance, err := protobuf.MarshalAny(c.Data)
		if err != nil {
			return err
		}
		if deleteRequestInstance == nil {
			return errors.New("nil")
		}
		deleteRequest := *deleteRequestInstance.(*pbkvs.DeleteRequest)

		return f.applyDelete(deleteRequest.Key)
	default:
		return errors.New("command type not support")
	}
}

func (f *RaftFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &KVSFSMSnapshot{
		kvs:    f.kvs,
		logger: f.logger,
	}, nil
}

func (f *RaftFSM) Restore(rc io.ReadCloser) error {
	defer func() {
		err := rc.Close()
		if err != nil {
			f.logger.Printf("[ERR] %v", err)
		}
	}()

	data, err := ioutil.ReadAll(rc)
	if err != nil {
		f.logger.Printf("[ERR] %v", err)
		return err
	}

	keyCount := 0
	buff := proto.NewBuffer(data)
	for {
		kvp := &pbkvs.KeyValuePair{}
		err = buff.DecodeMessage(kvp)
		if err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			f.logger.Printf("[ERR] %v", err)
			return err
		}

		// apply item to store
		err = f.kvs.Set(kvp.Key, kvp.Value)
		if err != nil {
			f.logger.Printf("[ERR] %v", err)
			return err
		}
		f.logger.Printf("[DEBUG] restore %v:%v", kvp.Key, kvp.Value)
		keyCount = keyCount + 1
	}

	f.logger.Printf("[INFO] %d keys were restored", keyCount)

	return nil
}

// ---------------------

type KVSFSMSnapshot struct {
	kvs    *KVS
	logger *log.Logger
}

func (f *KVSFSMSnapshot) Persist(sink raft.SnapshotSink) error {
	f.logger.Printf("[INFO] start data persistence")

	defer func() {
		err := sink.Close()
		if err != nil {
			f.logger.Printf("[ERR] %v", err)
		}
	}()

	ch := f.kvs.SnapshotItems()

	kvpCount := 0

	for {
		kvp := <-ch
		if kvp == nil {
			break
		}

		kvpCount = kvpCount + 1

		buff := proto.NewBuffer([]byte{})
		err := buff.EncodeMessage(kvp)
		if err != nil {
			return err
		}

		_, err = sink.Write(buff.Bytes())
		if err != nil {
			return err
		}
	}
	f.logger.Printf("[INFO] %d key-value pairs were persisted", kvpCount)

	return nil
}

func (f *KVSFSMSnapshot) Release() {
	f.logger.Printf("[INFO] release")
}
