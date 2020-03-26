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
	"context"
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
	certFile := ctx.String("cert-file")
	certHostname := ctx.String("cert-hostname")

	c, err := client.NewGRPCClientWithContextTLS(grpcAddr, context.Background(), certFile, certHostname)
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

			switch resp.Event.Type {
			case protobuf.Event_Join:
				eventReq := &protobuf.SetMetadataRequest{}
				if eventData, err := marshaler.MarshalAny(resp.Event.Data); err != nil {
					_, _ = fmt.Fprintln(os.Stderr, fmt.Sprintf("%s, %v", resp.Event.Type.String(), err))
				} else {
					if eventData == nil {
						_, _ = fmt.Fprintln(os.Stderr, fmt.Sprintf("%s, nil", resp.Event.Type.String()))
					} else {
						eventReq = eventData.(*protobuf.SetMetadataRequest)
					}
				}
				_, _ = fmt.Fprintln(os.Stdout, fmt.Sprintf("%s, %v", resp.Event.Type.String(), eventReq))
			case protobuf.Event_Leave:
				eventReq := &protobuf.DeleteMetadataRequest{}
				if eventData, err := marshaler.MarshalAny(resp.Event.Data); err != nil {
					_, _ = fmt.Fprintln(os.Stderr, fmt.Sprintf("%s, %v", resp.Event.Type.String(), err))
				} else {
					if eventData == nil {
						_, _ = fmt.Fprintln(os.Stderr, fmt.Sprintf("%s, nil", resp.Event.Type.String()))
					} else {
						eventReq = eventData.(*protobuf.DeleteMetadataRequest)
					}
				}
				_, _ = fmt.Fprintln(os.Stdout, fmt.Sprintf("%s, %v", resp.Event.Type.String(), eventReq))
			case protobuf.Event_Set:
				putRequest := &protobuf.SetRequest{}
				if putRequestInstance, err := marshaler.MarshalAny(resp.Event.Data); err != nil {
					_, _ = fmt.Fprintln(os.Stderr, fmt.Sprintf("%s, %v", resp.Event.Type.String(), err))
				} else {
					if putRequestInstance == nil {
						_, _ = fmt.Fprintln(os.Stderr, fmt.Sprintf("%s, nil", resp.Event.Type.String()))
					} else {
						putRequest = putRequestInstance.(*protobuf.SetRequest)
					}
				}
				_, _ = fmt.Fprintln(os.Stdout, fmt.Sprintf("%s, %v", resp.Event.Type.String(), putRequest))
			case protobuf.Event_Delete:
				deleteRequest := &protobuf.DeleteRequest{}
				if deleteRequestInstance, err := marshaler.MarshalAny(resp.Event.Data); err != nil {
					_, _ = fmt.Fprintln(os.Stderr, fmt.Sprintf("%s, %v", resp.Event.Type.String(), err))
				} else {
					if deleteRequestInstance == nil {
						_, _ = fmt.Fprintln(os.Stderr, fmt.Sprintf("%s, nil", resp.Event.Type.String()))
					} else {
						deleteRequest = deleteRequestInstance.(*protobuf.DeleteRequest)
					}
				}
				_, _ = fmt.Fprintln(os.Stdout, fmt.Sprintf("%s, %v", resp.Event.Type.String(), deleteRequest))
			}
		}
	}()

	quitCh := make(chan os.Signal, 1)
	signal.Notify(quitCh, os.Kill, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	<-quitCh

	return nil
}
