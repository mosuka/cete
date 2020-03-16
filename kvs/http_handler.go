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
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/mosuka/cete/errors"
	pbkvs "github.com/mosuka/cete/protobuf/kvs"
	"github.com/mosuka/cete/version"
	"go.uber.org/zap"
)

type RootHandler struct {
	logger *zap.Logger
}

func NewRootHandler(logger *zap.Logger) *RootHandler {
	return &RootHandler{
		logger: logger,
	}
}

func (h *RootHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	httpStatus := http.StatusOK

	content, _ := json.Marshal(
		map[string]interface{}{
			"version": version.Version,
		},
	)

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Length", strconv.FormatInt(int64(len(content)), 10))
	w.WriteHeader(httpStatus)
	_, _ = w.Write(content)
}

type GetHandler struct {
	client *GRPCClient
	logger *zap.Logger
}

func NewGetHandler(client *GRPCClient, logger *zap.Logger) *GetHandler {
	return &GetHandler{
		client: client,
		logger: logger,
	}
}

func (h *GetHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	req := &pbkvs.GetRequest{
		Key: []byte(vars["path"]),
	}

	content := make([]byte, 0)
	httpStatus := 0

	if resp, err := h.client.Get(req); err != nil {
		switch err {
		case errors.ErrNotFound:
			httpStatus = http.StatusNotFound
			h.logger.Error("not found", zap.String("key", string(req.Key)), zap.Error(err))
		default:
			content, _ = json.Marshal(
				map[string]interface{}{
					"error": err.Error(),
				},
			)
			httpStatus = http.StatusInternalServerError
			h.logger.Error("failed to get data", zap.String("key", string(req.Key)), zap.Error(err))
		}
	} else {
		content = resp.Value
		httpStatus = http.StatusOK
	}

	w.Header().Set("Content-Type", http.DetectContentType(content))
	w.Header().Set("Content-Length", strconv.FormatInt(int64(len(content)), 10))
	w.WriteHeader(httpStatus)
	_, _ = w.Write(content)
}

type PutHandler struct {
	client *GRPCClient
	logger *zap.Logger
}

func NewPutHandler(client *GRPCClient, logger *zap.Logger) *PutHandler {
	return &PutHandler{
		client: client,
		logger: logger,
	}
}

func (h *PutHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	content := make([]byte, 0)
	httpStatus := http.StatusOK

	if bodyBytes, err := ioutil.ReadAll(r.Body); err != nil {
		content, _ = json.Marshal(
			map[string]interface{}{
				"error": err.Error(),
			},
		)
		httpStatus = http.StatusInternalServerError
		h.logger.Error("failed to read data", zap.Error(err))
	} else {
		req := &pbkvs.PutRequest{
			Key:   []byte(vars["path"]),
			Value: bodyBytes,
		}
		if err = h.client.Put(req); err != nil {
			content, _ = json.Marshal(
				map[string]interface{}{
					"error": err.Error(),
				},
			)
			httpStatus = http.StatusInternalServerError
			h.logger.Error("failed to put data", zap.Error(err))
		} else {
			httpStatus = http.StatusOK
		}
	}

	w.Header().Set("Content-Type", http.DetectContentType(content))
	w.Header().Set("Content-Length", strconv.FormatInt(int64(len(content)), 10))
	w.WriteHeader(httpStatus)
	_, _ = w.Write(content)
}

type DeleteHandler struct {
	client *GRPCClient
	logger *zap.Logger
}

func NewDeleteHandler(client *GRPCClient, logger *zap.Logger) *DeleteHandler {
	return &DeleteHandler{
		client: client,
		logger: logger,
	}
}

func (h *DeleteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	req := &pbkvs.DeleteRequest{
		Key: []byte(vars["path"]),
	}

	content := make([]byte, 0)
	httpStatus := http.StatusOK

	if err := h.client.Delete(req); err != nil {
		content, _ = json.Marshal(
			map[string]interface{}{
				"error": err.Error(),
			},
		)
		httpStatus = http.StatusInternalServerError
		h.logger.Error("failed to delete data", zap.Error(err))
	} else {
		httpStatus = http.StatusOK
	}

	w.Header().Set("Content-Type", http.DetectContentType(content))
	w.Header().Set("Content-Length", strconv.FormatInt(int64(len(content)), 10))
	w.WriteHeader(httpStatus)
	_, _ = w.Write(content)
}
