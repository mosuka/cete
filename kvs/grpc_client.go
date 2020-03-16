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
	"context"
	"errors"
	"log"
	"math"

	"github.com/golang/protobuf/ptypes/empty"
	ceteerrors "github.com/mosuka/cete/errors"
	pbkvs "github.com/mosuka/cete/protobuf/kvs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GRPCClient struct {
	ctx    context.Context
	cancel context.CancelFunc
	conn   *grpc.ClientConn
	client pbkvs.KVSClient

	logger *log.Logger
}

func NewGRPCClient(address string) (*GRPCClient, error) {
	baseCtx := context.TODO()
	ctx, cancel := context.WithCancel(baseCtx)

	dialOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(math.MaxInt64),
			grpc.MaxCallRecvMsgSize(math.MaxInt64),
		),
	}

	conn, err := grpc.DialContext(ctx, address, dialOpts...)
	if err != nil {
		cancel()
		return nil, err
	}

	return &GRPCClient{
		ctx:    ctx,
		cancel: cancel,
		conn:   conn,
		client: pbkvs.NewKVSClient(conn),
	}, nil
}

func (c *GRPCClient) Close() error {
	c.cancel()
	if c.conn != nil {
		return c.conn.Close()
	}

	return c.ctx.Err()
}

func (c *GRPCClient) Join(req *pbkvs.JoinRequest, opts ...grpc.CallOption) error {
	_, err := c.client.Join(c.ctx, req, opts...)
	if err != nil {
		return err
	}

	return nil
}

func (c *GRPCClient) Leave(req *pbkvs.LeaveRequest, opts ...grpc.CallOption) error {
	_, err := c.client.Leave(c.ctx, req, opts...)
	if err != nil {
		return err
	}

	return nil
}

func (c *GRPCClient) Node(opts ...grpc.CallOption) (*pbkvs.NodeResponse, error) {
	resp, err := c.client.Node(c.ctx, &empty.Empty{}, opts...)
	if err != nil {
		st, _ := status.FromError(err)

		return nil, errors.New(st.Message())
	}

	return resp, nil
}

func (c *GRPCClient) Cluster(opts ...grpc.CallOption) (*pbkvs.ClusterResponse, error) {
	resp, err := c.client.Cluster(c.ctx, &empty.Empty{}, opts...)
	if err != nil {
		st, _ := status.FromError(err)

		return nil, errors.New(st.Message())
	}

	return resp, nil
}

func (c *GRPCClient) Snapshot(opts ...grpc.CallOption) error {
	_, err := c.client.Snapshot(c.ctx, &empty.Empty{})
	if err != nil {
		st, _ := status.FromError(err)

		return errors.New(st.Message())
	}

	return nil
}

func (c *GRPCClient) Get(req *pbkvs.GetRequest, opts ...grpc.CallOption) (*pbkvs.GetResponse, error) {
	resp, err := c.client.Get(c.ctx, req, opts...)
	if err != nil {
		st, _ := status.FromError(err)

		switch st.Code() {
		case codes.NotFound:
			return nil, ceteerrors.ErrNotFound
		default:
			return nil, errors.New(st.Message())
		}
	}

	return resp, nil
}

func (c *GRPCClient) Put(req *pbkvs.PutRequest, opts ...grpc.CallOption) error {
	_, err := c.client.Put(c.ctx, req, opts...)
	if err != nil {
		st, _ := status.FromError(err)

		return errors.New(st.Message())
	}

	return nil
}

func (c *GRPCClient) Delete(req *pbkvs.DeleteRequest, opts ...grpc.CallOption) error {
	_, err := c.client.Delete(c.ctx, req, opts...)
	if err != nil {
		st, _ := status.FromError(err)

		return errors.New(st.Message())
	}

	return nil
}

func (c *GRPCClient) Watch(req *empty.Empty, opts ...grpc.CallOption) (pbkvs.KVS_WatchClient, error) {
	return c.client.Watch(c.ctx, req, opts...)
}
