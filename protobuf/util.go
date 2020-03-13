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

package protobuf

import (
	"encoding/json"
	"reflect"

	"github.com/golang/protobuf/ptypes/any"
	pbkvs "github.com/mosuka/cete/protobuf/kvs"
	"github.com/mosuka/cete/registry"
)

func init() {
	registry.RegisterType("map[string]interface {}", reflect.TypeOf((map[string]interface{})(nil)))

	registry.RegisterType("kvs.JoinRequest", reflect.TypeOf(pbkvs.JoinRequest{}))
	registry.RegisterType("kvs.LeaveRequest", reflect.TypeOf(pbkvs.LeaveRequest{}))
	registry.RegisterType("kvs.GetRequest", reflect.TypeOf(pbkvs.GetRequest{}))
	registry.RegisterType("kvs.PutRequest", reflect.TypeOf(pbkvs.PutRequest{}))
	registry.RegisterType("kvs.DeleteRequest", reflect.TypeOf(pbkvs.DeleteRequest{}))
	registry.RegisterType("kvs.KeyValuePair", reflect.TypeOf(pbkvs.KeyValuePair{}))
	registry.RegisterType("kvs.Node", reflect.TypeOf(pbkvs.Node{}))
}

func MarshalAny(message *any.Any) (interface{}, error) {
	if message == nil {
		return nil, nil
	}

	typeUrl := message.TypeUrl
	value := message.Value

	instance := registry.TypeInstanceByName(typeUrl)

	err := json.Unmarshal(value, instance)
	if err != nil {
		return nil, err
	}

	return instance, nil
}

func UnmarshalAny(instance interface{}, message *any.Any) error {
	if instance == nil {
		return nil
	}

	value, err := json.Marshal(instance)
	if err != nil {
		return err
	}

	message.TypeUrl = registry.TypeNameByInstance(instance)
	message.Value = value

	return nil
}
