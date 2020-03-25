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
	"errors"

	"github.com/mosuka/cete/client"
	"github.com/mosuka/cete/protobuf"
	"github.com/urfave/cli"
)

func execLeave(ctx *cli.Context) error {
	grpcAddr := ctx.String("grpc-addr")
	certFile := ctx.String("cert-file")
	certHostname := ctx.String("cert-hostname")

	id := ctx.Args().Get(0)
	if id == "" {
		err := errors.New("id argument must be set")
		return err
	}

	req := &protobuf.LeaveRequest{
		Id: id,
	}

	c, err := client.NewGRPCClientWithContextTLS(grpcAddr, context.Background(), certFile, certHostname)
	if err != nil {
		return err
	}
	defer func() {
		_ = c.Close()
	}()

	err = c.Leave(req)
	if err != nil {
		return err
	}

	return nil
}
