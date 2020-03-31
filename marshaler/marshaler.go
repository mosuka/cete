package marshaler

import (
	"encoding/json"
	"io"
	"io/ioutil"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/mosuka/cete/protobuf"
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
	case *protobuf.GetResponse:
		value := v.(*protobuf.GetResponse).Value
		return value, nil
	case *protobuf.MetricsResponse:
		value := v.(*protobuf.MetricsResponse).Metrics
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
			case *protobuf.SetRequest:
				v.(*protobuf.SetRequest).Value = buffer
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
