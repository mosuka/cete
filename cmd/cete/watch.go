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
	"github.com/mosuka/cete/kvs"
	"github.com/mosuka/cete/protobuf"
	pbkvs "github.com/mosuka/cete/protobuf/kvs"
	"github.com/urfave/cli"
)

func execWatch(c *cli.Context) error {
	grpcAddr := c.String("grpc-addr")

	client, err := kvs.NewGRPCClient(grpcAddr)
	if err != nil {
		return err
	}
	defer func() {
		_ = client.Close()
	}()

	req := &empty.Empty{}
	watchClient, err := client.Watch(req)
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
			case pbkvs.WatchResponse_JOIN:
				joinRequest := &pbkvs.JoinRequest{}
				if joinRequestInstance, err := protobuf.MarshalAny(resp.Data); err != nil {
					_, _ = fmt.Fprintln(os.Stderr, fmt.Sprintf("%s, %v", resp.Event.String(), err))
				} else {
					if joinRequestInstance == nil {
						_, _ = fmt.Fprintln(os.Stderr, fmt.Sprintf("%s, nil", resp.Event.String()))
					} else {
						joinRequest = joinRequestInstance.(*pbkvs.JoinRequest)
					}
				}
				_, _ = fmt.Fprintln(os.Stdout, fmt.Sprintf("%s, %v", resp.Event.String(), joinRequest))
			case pbkvs.WatchResponse_LEAVE:
				leaveRequest := &pbkvs.LeaveRequest{}
				if leaveRequestInstance, err := protobuf.MarshalAny(resp.Data); err != nil {
					_, _ = fmt.Fprintln(os.Stderr, fmt.Sprintf("%s, %v", resp.Event.String(), err))
				} else {
					if leaveRequestInstance == nil {
						_, _ = fmt.Fprintln(os.Stderr, fmt.Sprintf("%s, nil", resp.Event.String()))
					} else {
						leaveRequest = leaveRequestInstance.(*pbkvs.LeaveRequest)
					}
				}
				_, _ = fmt.Fprintln(os.Stdout, fmt.Sprintf("%s, %v", resp.Event.String(), leaveRequest))
			case pbkvs.WatchResponse_PUT:
				putRequest := &pbkvs.PutRequest{}
				if putRequestInstance, err := protobuf.MarshalAny(resp.Data); err != nil {
					_, _ = fmt.Fprintln(os.Stderr, fmt.Sprintf("%s, %v", resp.Event.String(), err))
				} else {
					if putRequestInstance == nil {
						_, _ = fmt.Fprintln(os.Stderr, fmt.Sprintf("%s, nil", resp.Event.String()))
					} else {
						putRequest = putRequestInstance.(*pbkvs.PutRequest)
					}
				}
				_, _ = fmt.Fprintln(os.Stdout, fmt.Sprintf("%s, %v", resp.Event.String(), putRequest))
			case pbkvs.WatchResponse_DELETE:
				deleteRequest := &pbkvs.DeleteRequest{}
				if deleteRequestInstance, err := protobuf.MarshalAny(resp.Data); err != nil {
					_, _ = fmt.Fprintln(os.Stderr, fmt.Sprintf("%s, %v", resp.Event.String(), err))
				} else {
					if deleteRequestInstance == nil {
						_, _ = fmt.Fprintln(os.Stderr, fmt.Sprintf("%s, nil", resp.Event.String()))
					} else {
						deleteRequest = deleteRequestInstance.(*pbkvs.DeleteRequest)
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
