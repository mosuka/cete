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
	"encoding/json"
	"io"
	"io/ioutil"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	pbkvs "github.com/mosuka/cete/protobuf/kvs"
)

var (
	DefaultContentType = "application/json"
)

type CeteMarshaler struct{}

func (*CeteMarshaler) ContentType() string {
	return DefaultContentType
}

func (j *CeteMarshaler) Marshal(v interface{}) ([]byte, error) {
	switch v.(type) {
	case *pbkvs.GetResponse:
		value := v.(*pbkvs.GetResponse).Value
		return value, nil
	case *pbkvs.MetricsResponse:
		value := v.(*pbkvs.MetricsResponse).Metrics
		return value, nil
	default:
		return json.Marshal(v)
	}
}

func (j *CeteMarshaler) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func (j *CeteMarshaler) NewDecoder(r io.Reader) runtime.Decoder {
	return runtime.DecoderFunc(
		func(v interface{}) error {
			buffer, err := ioutil.ReadAll(r)
			if err != nil {
				return err
			}

			switch v.(type) {
			case *pbkvs.PutRequest:
				v.(*pbkvs.PutRequest).Value = buffer
				return nil
			default:
				return json.Unmarshal(buffer, v)
			}
		},
	)
}

func (j *CeteMarshaler) NewEncoder(w io.Writer) runtime.Encoder {
	return json.NewEncoder(w)
}

func (j *CeteMarshaler) Delimiter() []byte {
	return []byte("\n")
}
