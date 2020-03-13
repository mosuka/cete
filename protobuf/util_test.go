package protobuf

import (
	"bytes"
	"testing"

	"github.com/golang/protobuf/ptypes/any"
	"github.com/mosuka/cete/protobuf/kvs"
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
	node := &kvs.Node{
		GrpcAddr: ":5050",
		BindAddr: ":6060",
		State:    "Leader",
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

	expectedValue = []byte(`{"bind_addr":":6060","grpc_addr":":5050","state":"Leader"}`)
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
		TypeUrl: "kvs.Node",
		Value:   []byte(`{"bind_addr":":6060","grpc_addr":":5050","state":"Leader"}`),
	}

	data, err = MarshalAny(dataAny)
	if err != nil {
		t.Errorf("%v", err)
	}
	dataNode := data.(*kvs.Node)

	if dataNode.BindAddr != ":6060" {
		t.Errorf("expected content to see %v, saw %v", ":6060", dataNode.BindAddr)
	}
	if dataNode.GrpcAddr != ":5050" {
		t.Errorf("expected content to see %v, saw %v", ":5050", dataNode.BindAddr)
	}
	if dataNode.State != "Leader" {
		t.Errorf("expected content to see %v, saw %v", "Leader", dataNode.State)
	}
}
