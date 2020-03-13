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
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/mosuka/cete/errors"
	cetehttp "github.com/mosuka/cete/http"
	pbkvs "github.com/mosuka/cete/protobuf/kvs"
	"github.com/mosuka/cete/version"
)

type RootHandler struct {
	logger *log.Logger
}

func NewRootHandler(logger *log.Logger) *RootHandler {
	return &RootHandler{
		logger: logger,
	}
}

func (h *RootHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	status := http.StatusOK
	content := make([]byte, 0)
	defer func() {
		cetehttp.WriteResponse(w, content, status, h.logger)
		cetehttp.RecordMetrics(start, status, w, r, h.logger)
	}()

	msgMap := map[string]interface{}{
		"version": version.Version,
		"status":  status,
	}

	content, err := cetehttp.NewJSONMessage(msgMap)
	if err != nil {
		h.logger.Printf("[ERR] %v", err)
	}
}

type GetHandler struct {
	client *GRPCClient
	logger *log.Logger
}

func NewGetHandler(client *GRPCClient, logger *log.Logger) *GetHandler {
	return &GetHandler{
		client: client,
		logger: logger,
	}
}

func (h *GetHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	httpStatus := http.StatusOK
	content := make([]byte, 0)
	defer func() {
		cetehttp.WriteResponse(w, content, httpStatus, h.logger)
		cetehttp.RecordMetrics(start, httpStatus, w, r, h.logger)
	}()

	vars := mux.Vars(r)

	kvp := &pbkvs.GetRequest{
		Key: []byte(vars["path"]),
	}

	retKVP, err := h.client.Get(kvp)
	if err != nil {
		switch err {
		case errors.ErrNotFound:
			httpStatus = http.StatusNotFound
		default:
			httpStatus = http.StatusInternalServerError
		}

		msgMap := map[string]interface{}{
			"message": err.Error(),
			"status":  httpStatus,
		}

		content, err = cetehttp.NewJSONMessage(msgMap)
		if err != nil {
			h.logger.Printf("[ERR] %v", err)
		}

		return
	}

	content = retKVP.Value
}

type PutHandler struct {
	client *GRPCClient
	logger *log.Logger
}

func NewPutHandler(client *GRPCClient, logger *log.Logger) *PutHandler {
	return &PutHandler{
		client: client,
		logger: logger,
	}
}

func (h *PutHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	httpStatus := http.StatusOK
	content := make([]byte, 0)
	defer func() {
		cetehttp.WriteResponse(w, content, httpStatus, h.logger)
		cetehttp.RecordMetrics(start, httpStatus, w, r, h.logger)
	}()

	vars := mux.Vars(r)

	bodyBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		httpStatus = http.StatusInternalServerError

		msgMap := map[string]interface{}{
			"message": err.Error(),
			"status":  httpStatus,
		}

		content, err = cetehttp.NewJSONMessage(msgMap)
		if err != nil {
			h.logger.Printf("[ERR] %v", err)
		}

		return
	}

	kvp := &pbkvs.PutRequest{
		Key:   []byte(vars["path"]),
		Value: bodyBytes,
	}

	err = h.client.Put(kvp)
	if err != nil {
		httpStatus = http.StatusInternalServerError

		msgMap := map[string]interface{}{
			"message": err.Error(),
			"status":  httpStatus,
		}

		content, err = cetehttp.NewJSONMessage(msgMap)
		if err != nil {
			h.logger.Printf("[ERR] %v", err)
		}

		return
	}
}

type DeleteHandler struct {
	client *GRPCClient
	logger *log.Logger
}

func NewDeleteHandler(client *GRPCClient, logger *log.Logger) *DeleteHandler {
	return &DeleteHandler{
		client: client,
		logger: logger,
	}
}

func (h *DeleteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	httpStatus := http.StatusOK
	content := make([]byte, 0)
	defer func() {
		cetehttp.WriteResponse(w, content, httpStatus, h.logger)
		cetehttp.RecordMetrics(start, httpStatus, w, r, h.logger)
	}()

	vars := mux.Vars(r)

	kvp := &pbkvs.DeleteRequest{
		Key: []byte(vars["path"]),
	}

	err := h.client.Delete(kvp)
	if err != nil {
		httpStatus = http.StatusInternalServerError

		msgMap := map[string]interface{}{
			"message": err.Error(),
			"status":  httpStatus,
		}

		content, err = cetehttp.NewJSONMessage(msgMap)
		if err != nil {
			h.logger.Printf("[ERR] %v", err)
		}

		return
	}
}
