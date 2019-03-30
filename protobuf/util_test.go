package protobuf

import (
	"bytes"
	"testing"

	"github.com/golang/protobuf/ptypes/any"
	"github.com/mosuka/cete/protobuf/raft"
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

	// test raft.Node
	node := &raft.Node{
		Id:       "node1",
		GrpcAddr: ":5050",
		DataDir:  "/tmp/blast/index1",
		BindAddr: ":6060",
		HttpAddr: ":8080",
		Leader:   true,
	}

	nodeAny := &any.Any{}
	err = UnmarshalAny(node, nodeAny)
	if err != nil {
		t.Errorf("%v", err)
	}

	expectedType = "raft.Node"
	actualType = nodeAny.TypeUrl
	if expectedType != actualType {
		t.Errorf("expected content to see %s, saw %s", expectedType, actualType)
	}

	expectedValue = []byte(`{"id":"node1","bind_addr":":6060","grpc_addr":":5050","http_addr":":8080","leader":true,"data_dir":"/tmp/blast/index1"}`)
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
		TypeUrl: "raft.Node",
		Value:   []byte(`{"id":"node1","bind_addr":":6060","grpc_addr":":5050","http_addr":":8080","leader":true,"data_dir":"/tmp/blast/index1"}`),
	}

	data, err = MarshalAny(dataAny)
	if err != nil {
		t.Errorf("%v", err)
	}
	dataNode := data.(*raft.Node)

	if dataNode.Id != "node1" {
		t.Errorf("expected content to see %v, saw %v", "node1", dataNode.Id)
	}
	if dataNode.HttpAddr != ":8080" {
		t.Errorf("expected content to see %v, saw %v", ":8080", dataNode.HttpAddr)
	}
	if dataNode.BindAddr != ":6060" {
		t.Errorf("expected content to see %v, saw %v", ":6060", dataNode.BindAddr)
	}
	if dataNode.GrpcAddr != ":5050" {
		t.Errorf("expected content to see %v, saw %v", ":5050", dataNode.BindAddr)
	}
	if dataNode.DataDir != "/tmp/blast/index1" {
		t.Errorf("expected content to see %v, saw %v", "/tmp/blast/index1", dataNode.DataDir)
	}
	if dataNode.Leader != true {
		t.Errorf("expected content to see %v, saw %v", true, dataNode.Leader)
	}
}
