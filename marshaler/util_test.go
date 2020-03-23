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

package marshaler

import (
	"bytes"
	"testing"

	"github.com/golang/protobuf/ptypes/any"
	"github.com/mosuka/cete/protobuf"
)

func TestMarshalAny(t *testing.T) {
	// test map[string]interface{}
	data := map[string]interface{}{"a": 1, "b": 2, "c": 3}

	mapAny := &any.Any{}
	err := UnmarshalAny(data, mapAny)
	if err != nil {
		t.Errorf("%v", err)
	}

	expectedType := "map[string]interface {}"
	actualType := mapAny.TypeUrl
	if expectedType != actualType {
		t.Errorf("expected content to see %s, saw %s", expectedType, actualType)
	}

	expectedValue := []byte(`{"a":1,"b":2,"c":3}`)
	actualValue := mapAny.Value
	if !bytes.Equal(expectedValue, actualValue) {
		t.Errorf("expected content to see %v, saw %v", expectedValue, actualValue)
	}

	// test kvs.Node
	node := &protobuf.Node{
		BindAddr: ":7000",
		State:    "Leader",
		Metadata: &protobuf.Metadata{
			GrpcAddr: ":9000",
			HttpAddr: ":8000",
		},
	}

	nodeAny := &any.Any{}
	err = UnmarshalAny(node, nodeAny)
	if err != nil {
		t.Errorf("%v", err)
	}

	expectedType = "kvs.Node"
	actualType = nodeAny.TypeUrl
	if expectedType != actualType {
		t.Errorf("expected content to see %s, saw %s", expectedType, actualType)
	}

	expectedValue = []byte(`{"bind_addr":":7000","state":"Leader","metadata":{"grpc_addr":":9000","http_addr":":8000",}}`)
	actualValue = nodeAny.Value
	if !bytes.Equal(expectedValue, actualValue) {
		t.Errorf("expected content to see %v, saw %v", expectedValue, actualValue)
	}
}

func TestUnmarshalAny(t *testing.T) {
	// test map[string]interface{}
	dataAny := &any.Any{
		TypeUrl: "map[string]interface {}",
		Value:   []byte(`{"a":1,"b":2,"c":3}`),
	}

	data, err := MarshalAny(dataAny)
	if err != nil {
		t.Errorf("%v", err)
	}
	dataMap := *data.(*map[string]interface{})

	if dataMap["a"] != float64(1) {
		t.Errorf("expected content to see %v, saw %v", 1, dataMap["a"])
	}
	if dataMap["b"] != float64(2) {
		t.Errorf("expected content to see %v, saw %v", 2, dataMap["b"])
	}
	if dataMap["c"] != float64(3) {
		t.Errorf("expected content to see %v, saw %v", 3, dataMap["c"])
	}

	// raft.Node
	dataAny = &any.Any{
		TypeUrl: "protobuf.Node",
		Value:   []byte(`{"bind_addr":":7000","state":"Leader","metadata":{"grpc_addr":":9000","http_addr":":8000",}}`),
	}

	data, err = MarshalAny(dataAny)
	if err != nil {
		t.Errorf("%v", err)
	}
	node := data.(*protobuf.Node)

	if node.BindAddr != ":6060" {
		t.Errorf("expected content to see %v, saw %v", ":6060", node.BindAddr)
	}
	if node.Metadata.GrpcAddr != ":9000" {
		t.Errorf("expected content to see %v, saw %v", ":5050", node.BindAddr)
	}
	if node.Metadata.HttpAddr != ":8000" {
		t.Errorf("expected content to see %v, saw %v", ":5050", node.BindAddr)
	}
	if node.State != "Leader" {
		t.Errorf("expected content to see %v, saw %v", "Leader", node.State)
	}
}
