package storage

import (
	"context"
	"time"

	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/y"
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

func (k *KVS) Close() error {
	if err := k.db.Close(); err != nil {
		k.logger.Error("failed to close database", zap.Error(err))
		return err
	}

	return nil
}

func (k *KVS) RunGC(ctx context.Context, interval time.Duration, discardRatio float64) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				start := time.Now()

				for {
					err := k.db.RunValueLogGC(discardRatio)
					if err != nil {
						if err == badger.ErrNoRewrite {
							break
						}

						k.logger.Error("garbage collection failed", zap.Error(err))
						break
					}
				}

				k.logger.Info("garbage collection finished", zap.Float64("time", float64(time.Since(start))/float64(time.Second)))
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (k *KVS) Get(key string) ([]byte, error) {
	start := time.Now()

	var value []byte
	if err := k.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			k.logger.Error("failed to get item", zap.String("key", key), zap.Error(err))
			return err
		}

		err = item.Value(func(val []byte) error {
			value = append([]byte{}, val...)
			return nil
		})
		if err != nil {
			k.logger.Error("failed to get item value", zap.String("key", key), zap.Error(err))
			return err
		}

		return nil
	}); err == badger.ErrKeyNotFound {
		k.logger.Debug("not found", zap.String("key", key), zap.Error(err))
		return nil, errors.ErrNotFound
	} else if err != nil {
		k.logger.Error("failed to get value", zap.String("key", key), zap.Error(err))
		return nil, err
	}

	k.logger.Debug("get", zap.String("key", key), zap.Float64("time", float64(time.Since(start))/float64(time.Second)))
	return value, nil
}

func (k *KVS) Scan(prefix string) ([][]byte, error) {
	start := time.Now()

	var value [][]byte
	if err := k.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefixBytes := []byte(prefix)
		for it.Seek(prefixBytes); it.ValidForPrefix(prefixBytes); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				value = append(value, append([]byte{}, val...))
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		k.logger.Error("failed to scan value", zap.String("prefix", prefix), zap.Error(err))
		return nil, err
	}

	k.logger.Debug("scan", zap.String("prefix", prefix), zap.Float64("time", float64(time.Since(start))/float64(time.Second)))
	return value, nil
}

func (k *KVS) Set(key string, value []byte) error {
	start := time.Now()

	if err := k.db.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(key), value)
		if err != nil {
			k.logger.Error("failed to set item", zap.String("key", key), zap.Error(err))
			return err
		}
		return nil
	}); err != nil {
		k.logger.Error("failed to set value", zap.String("key", key), zap.Error(err))
		return err
	}

	k.logger.Debug("set", zap.String("key", key), zap.Float64("time", float64(time.Since(start))/float64(time.Second)))
	return nil
}

func (k *KVS) Delete(key string) error {
	start := time.Now()

	if err := k.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete([]byte(key))
		if err != nil {
			k.logger.Error("failed to delete item", zap.String("key", key), zap.Error(err))
			return err
		}
		return nil
	}); err != nil {
		k.logger.Error("failed to delete value", zap.String("key", key), zap.Error(err))
		return err
	}

	k.logger.Debug("delete", zap.String("key", key), zap.Float64("time", float64(time.Since(start))/float64(time.Second)))
	return nil
}

func (k *KVS) Stats() map[string]string {
	stats := map[string]string{}

	stats["num_reads"] = y.NumReads.String()
	stats["num_writes"] = y.NumWrites.String()
	stats["num_bytes_read"] = y.NumBytesRead.String()
	stats["num_bytes_written"] = y.NumBytesWritten.String()
	stats["num_lsm_gets"] = y.NumLSMGets.String()
	stats["num_lsm_bloom_Hits"] = y.NumLSMBloomHits.String()
	stats["num_gets"] = y.NumGets.String()
	stats["num_puts"] = y.NumPuts.String()
	stats["num_blocked_puts"] = y.NumBlockedPuts.String()
	stats["num_memtables_gets"] = y.NumMemtableGets.String()
	stats["lsm_size"] = y.LSMSize.String()
	stats["vlog_size"] = y.VlogSize.String()
	stats["pending_writes"] = y.PendingWrites.String()

	return stats
}

func (k *KVS) SnapshotItems() <-chan *protobuf.KeyValuePair {
	ch := make(chan *protobuf.KeyValuePair, 1024)

	go func() {
		start := time.Now()

		k.logger.Info("start to snapshot items")

		keyCount := uint64(0)

		if err := k.db.View(func(txn *badger.Txn) error {
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
					k.logger.Error("failed to get item value", zap.String("key", key), zap.Error(err))
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
			k.logger.Error("failed to snapshot items", zap.Error(err))
			return
		}

		k.logger.Info("finished to snapshot items", zap.Uint64("count", keyCount), zap.Float64("time", float64(time.Since(start))/float64(time.Second)))
	}()

	return ch
}
