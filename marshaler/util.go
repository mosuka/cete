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
	"encoding/json"
	"reflect"

	"github.com/golang/protobuf/ptypes/any"
	"github.com/mosuka/cete/protobuf"
	"github.com/mosuka/cete/registry"
)

func init() {
	registry.RegisterType("protobuf.JoinRequest", reflect.TypeOf(protobuf.JoinRequest{}))
	registry.RegisterType("protobuf.LeaveRequest", reflect.TypeOf(protobuf.LeaveRequest{}))
	registry.RegisterType("protobuf.GetRequest", reflect.TypeOf(protobuf.GetRequest{}))
	registry.RegisterType("protobuf.SetRequest", reflect.TypeOf(protobuf.SetRequest{}))
	registry.RegisterType("protobuf.DeleteRequest", reflect.TypeOf(protobuf.DeleteRequest{}))
	registry.RegisterType("protobuf.KeyValuePair", reflect.TypeOf(protobuf.KeyValuePair{}))
	registry.RegisterType("protobuf.SetMetadataRequest", reflect.TypeOf(protobuf.SetMetadataRequest{}))
	registry.RegisterType("protobuf.DeleteMetadataRequest", reflect.TypeOf(protobuf.DeleteMetadataRequest{}))
	registry.RegisterType("protobuf.Metadata", reflect.TypeOf(protobuf.Metadata{}))
	registry.RegisterType("protobuf.Node", reflect.TypeOf(protobuf.Node{}))
	registry.RegisterType("map[string]interface {}", reflect.TypeOf((map[string]interface{})(nil)))
}

func MarshalAny(message *any.Any) (interface{}, error) {
	if message == nil {
		return nil, nil
	}

	typeUrl := message.TypeUrl
	value := message.Value

	instance := registry.TypeInstanceByName(typeUrl)

	if err := json.Unmarshal(value, instance); err != nil {
		return nil, err
	} else {
		return instance, nil
	}

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
