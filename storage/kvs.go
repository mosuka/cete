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

package storage

import (
	"time"

	"github.com/dgraph-io/badger/v2"
	"github.com/mosuka/cete/errors"
	"github.com/mosuka/cete/protobuf"
	"go.uber.org/zap"
)

type KVS struct {
	dir      string
	valueDir string
	db       *badger.DB
	logger   *zap.Logger
}

func NewKVS(dir string, valueDir string, logger *zap.Logger) (*KVS, error) {
	opts := badger.DefaultOptions(dir)
	opts.ValueDir = valueDir
	opts.SyncWrites = false
	opts.Logger = nil

	db, err := badger.Open(opts)
	if err != nil {
		logger.Error("failed to open database", zap.Any("opts", opts), zap.Error(err))
		return nil, err
	}

	return &KVS{
		dir:      dir,
		valueDir: valueDir,
		db:       db,
		logger:   logger,
	}, nil
}

func (b *KVS) Close() error {
	if err := b.db.Close(); err != nil {
		b.logger.Error("failed to close database", zap.Error(err))
		return err
	}

	return nil
}

func (b *KVS) Get(key string) ([]byte, error) {
	start := time.Now()

	var value []byte
	if err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			b.logger.Error("failed to get item", zap.String("key", key), zap.Error(err))
			return err
		}

		err = item.Value(func(val []byte) error {
			value = append([]byte{}, val...)
			return nil
		})
		if err != nil {
			b.logger.Error("failed to get item value", zap.String("key", key), zap.Error(err))
			return err
		}

		return nil
	}); err == badger.ErrKeyNotFound {
		b.logger.Debug("not found", zap.String("key", key), zap.Error(err))
		return nil, errors.ErrNotFound
	} else if err != nil {
		b.logger.Error("failed to get value", zap.String("key", key), zap.Error(err))
		return nil, err
	}

	b.logger.Debug("get", zap.String("key", key), zap.Float64("time", float64(time.Since(start))/float64(time.Second)))
	return value, nil
}

func (b *KVS) Set(key string, value []byte) error {
	start := time.Now()

	if err := b.db.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(key), value)
		if err != nil {
			b.logger.Error("failed to set item", zap.String("key", key), zap.Error(err))
			return err
		}
		return nil
	}); err != nil {
		b.logger.Error("failed to set value", zap.String("key", key), zap.Error(err))
		return err
	}

	b.logger.Debug("set", zap.String("key", key), zap.Float64("time", float64(time.Since(start))/float64(time.Second)))
	return nil
}

func (b *KVS) Delete(key string) error {
	start := time.Now()

	if err := b.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete([]byte(key))
		if err != nil {
			b.logger.Error("failed to delete item", zap.String("key", key), zap.Error(err))
			return err
		}
		return nil
	}); err != nil {
		b.logger.Error("failed to delete value", zap.String("key", key), zap.Error(err))
		return err
	}

	b.logger.Debug("delete", zap.String("key", key), zap.Float64("time", float64(time.Since(start))/float64(time.Second)))
	return nil
}

func (b *KVS) SnapshotItems() <-chan *protobuf.KeyValuePair {
	ch := make(chan *protobuf.KeyValuePair, 1024)

	go func() {
		start := time.Now()

		b.logger.Info("start to snapshot items")

		keyCount := uint64(0)

		if err := b.db.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchSize = 10
			it := txn.NewIterator(opts)
			defer it.Close()

			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				key := string(item.Key())

				var value []byte
				if err := item.Value(func(val []byte) error {
					value = append([]byte{}, val...)
					return nil
				}); err != nil {
					b.logger.Error("failed to get item value", zap.String("key", key), zap.Error(err))
					return err
				}

				ch <- &protobuf.KeyValuePair{
					Key:   key,
					Value: append([]byte{}, value...),
				}

				keyCount = keyCount + 1
			}
			ch <- nil
			return nil
		}); err != nil {
			b.logger.Error("failed to snapshot items", zap.Error(err))
			return
		}

		b.logger.Info("finished to snapshot items", zap.Uint64("count", keyCount), zap.Float64("time", float64(time.Since(start))/float64(time.Second)))
	}()

	return ch
}
