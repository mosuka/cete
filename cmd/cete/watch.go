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

package main

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/mosuka/cete/client"
	"github.com/mosuka/cete/marshaler"
	"github.com/mosuka/cete/protobuf"
	"github.com/urfave/cli"
)

func execWatch(ctx *cli.Context) error {
	grpcAddr := ctx.String("grpc-addr")

	c, err := client.NewGRPCClient(grpcAddr)
	if err != nil {
		return err
	}
	defer func() {
		_ = c.Close()
	}()

	req := &empty.Empty{}
	watchClient, err := c.Watch(req)
	if err != nil {
		return err
	}

	go func() {
		for {
			resp, err := watchClient.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				break
			}

			switch resp.Event {
			case protobuf.WatchResponse_JOIN:
				joinRequest := &protobuf.JoinRequest{}
				if joinRequestInstance, err := marshaler.MarshalAny(resp.Data); err != nil {
					_, _ = fmt.Fprintln(os.Stderr, fmt.Sprintf("%s, %v", resp.Event.String(), err))
				} else {
					if joinRequestInstance == nil {
						_, _ = fmt.Fprintln(os.Stderr, fmt.Sprintf("%s, nil", resp.Event.String()))
					} else {
						joinRequest = joinRequestInstance.(*protobuf.JoinRequest)
					}
				}
				_, _ = fmt.Fprintln(os.Stdout, fmt.Sprintf("%s, %v", resp.Event.String(), joinRequest))
			case protobuf.WatchResponse_LEAVE:
				leaveRequest := &protobuf.LeaveRequest{}
				if leaveRequestInstance, err := marshaler.MarshalAny(resp.Data); err != nil {
					_, _ = fmt.Fprintln(os.Stderr, fmt.Sprintf("%s, %v", resp.Event.String(), err))
				} else {
					if leaveRequestInstance == nil {
						_, _ = fmt.Fprintln(os.Stderr, fmt.Sprintf("%s, nil", resp.Event.String()))
					} else {
						leaveRequest = leaveRequestInstance.(*protobuf.LeaveRequest)
					}
				}
				_, _ = fmt.Fprintln(os.Stdout, fmt.Sprintf("%s, %v", resp.Event.String(), leaveRequest))
			case protobuf.WatchResponse_PUT:
				putRequest := &protobuf.PutRequest{}
				if putRequestInstance, err := marshaler.MarshalAny(resp.Data); err != nil {
					_, _ = fmt.Fprintln(os.Stderr, fmt.Sprintf("%s, %v", resp.Event.String(), err))
				} else {
					if putRequestInstance == nil {
						_, _ = fmt.Fprintln(os.Stderr, fmt.Sprintf("%s, nil", resp.Event.String()))
					} else {
						putRequest = putRequestInstance.(*protobuf.PutRequest)
					}
				}
				_, _ = fmt.Fprintln(os.Stdout, fmt.Sprintf("%s, %v", resp.Event.String(), putRequest))
			case protobuf.WatchResponse_DELETE:
				deleteRequest := &protobuf.DeleteRequest{}
				if deleteRequestInstance, err := marshaler.MarshalAny(resp.Data); err != nil {
					_, _ = fmt.Fprintln(os.Stderr, fmt.Sprintf("%s, %v", resp.Event.String(), err))
				} else {
					if deleteRequestInstance == nil {
						_, _ = fmt.Fprintln(os.Stderr, fmt.Sprintf("%s, nil", resp.Event.String()))
					} else {
						deleteRequest = deleteRequestInstance.(*protobuf.DeleteRequest)
					}
				}
				_, _ = fmt.Fprintln(os.Stdout, fmt.Sprintf("%s, %v", resp.Event.String(), deleteRequest))
			}
		}
	}()

	quitCh := make(chan os.Signal, 1)
	signal.Notify(quitCh, os.Kill, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	<-quitCh

	return nil
}
