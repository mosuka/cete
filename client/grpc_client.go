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

package client

import (
	"context"
	"log"
	"math"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/mosuka/cete/errors"
	"github.com/mosuka/cete/protobuf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

type GRPCClient struct {
	ctx    context.Context
	cancel context.CancelFunc
	conn   *grpc.ClientConn
	client protobuf.KVSClient

	logger *log.Logger
}

func NewGRPCClient(address string) (*GRPCClient, error) {
	return NewGRPCClientWithContext(address, context.TODO())
}

func NewGRPCClientWithContext(address string, baseCtx context.Context) (*GRPCClient, error) {
	ctx, cancel := context.WithCancel(baseCtx)

	dialOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(math.MaxInt64),
			grpc.MaxCallRecvMsgSize(math.MaxInt64),
		),
		grpc.WithKeepaliveParams(
			keepalive.ClientParameters{
				Time:                1 * time.Second,
				Timeout:             5 * time.Second,
				PermitWithoutStream: true,
			},
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
		client: protobuf.NewKVSClient(conn),
	}, nil
}

func (c *GRPCClient) Close() error {
	c.cancel()
	if c.conn != nil {
		return c.conn.Close()
	}

	return c.ctx.Err()
}

func (c *GRPCClient) Target() string {
	return c.conn.Target()
}

func (c *GRPCClient) Join(req *protobuf.JoinRequest, opts ...grpc.CallOption) error {
	if _, err := c.client.Join(c.ctx, req, opts...); err != nil {
		return err
	}

	return nil
}

func (c *GRPCClient) Leave(req *protobuf.LeaveRequest, opts ...grpc.CallOption) error {
	if _, err := c.client.Leave(c.ctx, req, opts...); err != nil {
		return err
	}

	return nil
}

func (c *GRPCClient) Node(opts ...grpc.CallOption) (*protobuf.NodeResponse, error) {
	if resp, err := c.client.Node(c.ctx, &empty.Empty{}, opts...); err != nil {
		return nil, err
	} else {
		return resp, nil
	}
}

func (c *GRPCClient) Cluster(opts ...grpc.CallOption) (*protobuf.ClusterResponse, error) {
	if resp, err := c.client.Cluster(c.ctx, &empty.Empty{}, opts...); err != nil {
		return nil, err
	} else {
		return resp, nil
	}
}

func (c *GRPCClient) Snapshot(opts ...grpc.CallOption) error {
	if _, err := c.client.Snapshot(c.ctx, &empty.Empty{}); err != nil {
		return err
	}

	return nil
}

func (c *GRPCClient) Get(req *protobuf.GetRequest, opts ...grpc.CallOption) (*protobuf.GetResponse, error) {
	if resp, err := c.client.Get(c.ctx, req, opts...); err != nil {
		st, _ := status.FromError(err)
		switch st.Code() {
		case codes.NotFound:
			return nil, errors.ErrNotFound
		default:
			return nil, err
		}
	} else {
		return resp, nil
	}
}

func (c *GRPCClient) Set(req *protobuf.SetRequest, opts ...grpc.CallOption) error {
	if _, err := c.client.Set(c.ctx, req, opts...); err != nil {
		return err
	}

	return nil
}

func (c *GRPCClient) Delete(req *protobuf.DeleteRequest, opts ...grpc.CallOption) error {
	if _, err := c.client.Delete(c.ctx, req, opts...); err != nil {
		return err
	}

	return nil
}

func (c *GRPCClient) Watch(req *empty.Empty, opts ...grpc.CallOption) (protobuf.KVS_WatchClient, error) {
	return c.client.Watch(c.ctx, req, opts...)
}

func (c *GRPCClient) Metrics(opts ...grpc.CallOption) (*protobuf.MetricsResponse, error) {
	if resp, err := c.client.Metrics(c.ctx, &empty.Empty{}, opts...); err != nil {
		return nil, err
	} else {
		return resp, nil
	}
}
