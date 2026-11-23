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
	nodeID   string
	db       *badger.DB
	logger   *zap.Logger
}

const maxTxnRetries = 10

func NewKVS(dir string, valueDir string, nodeID string, logger *zap.Logger) (*KVS, error) {
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
		nodeID:   nodeID,
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

	var stored []byte
	if err := k.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			k.logger.Error("failed to get item", zap.String("key", key), zap.Error(err))
			return err
		}

		err = item.Value(func(val []byte) error {
			stored = append([]byte{}, val...)
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

	value := unwrapVersioned(stored)

	k.logger.Debug("get", zap.String("key", key), zap.Float64("time", float64(time.Since(start))/float64(time.Second)))
	return value.Value, nil
}

// GetVersioned returns the value together with its causal history.
func (k *KVS) GetVersioned(key string) (*VersionedValue, error) {
	var stored []byte
	if err := k.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			stored = append([]byte{}, val...)
			return nil
		})
	}); err == badger.ErrKeyNotFound {
		return nil, errors.ErrNotFound
	} else if err != nil {
		k.logger.Error("failed to get value", zap.String("key", key), zap.Error(err))
		return nil, err
	}

	value := unwrapVersioned(stored)
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
				versioned := unwrapVersioned(append([]byte{}, val...))
				value = append(value, versioned.Value)
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

	// The read-modify-write below can lose Badger's optimistic concurrency
	// check when clients write the same key concurrently; retrying is safe
	// because the version is recomputed from the winning transaction's state.
	var err error
	for attempt := 0; attempt < maxTxnRetries; attempt++ {
		err = k.db.Update(func(txn *badger.Txn) error {
			// Carry the causal history of the previous version forward so that
			// writes serialized through the Raft log always dominate their
			// predecessors and are never mistaken for conflicts.
			clock := VClock{}
			if item, err := txn.Get([]byte(key)); err == nil {
				if err := item.Value(func(val []byte) error {
					clock = unwrapVersioned(val).Clock
					return nil
				}); err != nil {
					k.logger.Error("failed to get item value", zap.String("key", key), zap.Error(err))
					return err
				}
			} else if err != badger.ErrKeyNotFound {
				k.logger.Error("failed to get item", zap.String("key", key), zap.Error(err))
				return err
			}

			stored, err := encodeVersioned(&VersionedValue{
				Value:     value,
				Clock:     clock.Copy().Increment(k.nodeID),
				Timestamp: time.Now().UnixNano(),
				NodeID:    k.nodeID,
			})
			if err != nil {
				k.logger.Error("failed to encode versioned value", zap.String("key", key), zap.Error(err))
				return err
			}

			err = txn.Set([]byte(key), stored)
			if err != nil {
				k.logger.Error("failed to set item", zap.String("key", key), zap.Error(err))
				return err
			}
			return nil
		})
		if err != badger.ErrConflict {
			break
		}
	}
	if err != nil {
		k.logger.Error("failed to set value", zap.String("key", key), zap.Error(err))
		return err
	}

	k.logger.Debug("set", zap.String("key", key), zap.Float64("time", float64(time.Since(start))/float64(time.Second)))
	return nil
}

// SetRaw stores already-encoded bytes for a key without touching their
// version. It is used to restore snapshots and to apply the outcome of a
// reconciliation, where the version must be preserved as-is.
func (k *KVS) SetRaw(key string, stored []byte) error {
	if err := k.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), stored)
	}); err != nil {
		k.logger.Error("failed to set raw value", zap.String("key", key), zap.Error(err))
		return err
	}

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

// VersionedItems streams every key together with its version, for
// reconciliation with another replica after a partition heals.
// A nil item marks the end of the stream.
func (k *KVS) VersionedItems() <-chan *VersionedKeyValue {
	ch := make(chan *VersionedKeyValue, 1024)

	go func() {
		defer close(ch)

		start := time.Now()

		k.logger.Info("start to export versioned items")

		keyCount := uint64(0)

		if err := k.db.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchSize = 10
			it := txn.NewIterator(opts)
			defer it.Close()

			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				key := string(item.Key())

				var stored []byte
				if err := item.Value(func(val []byte) error {
					stored = append([]byte{}, val...)
					return nil
				}); err != nil {
					k.logger.Error("failed to get item value", zap.String("key", key), zap.Error(err))
					return err
				}

				ch <- &VersionedKeyValue{
					Key:   key,
					Value: unwrapVersioned(stored),
				}

				keyCount = keyCount + 1
			}
			ch <- nil
			return nil
		}); err != nil {
			k.logger.Error("failed to export versioned items", zap.Error(err))
			return
		}

		k.logger.Info("finished to export versioned items", zap.Uint64("count", keyCount), zap.Float64("time", float64(time.Since(start))/float64(time.Second)))
	}()

	return ch
}

// Reconcile merges a stream of versioned items received from another replica
// into the local store and returns the genuine conflicts it encountered.
//
// For every key the causal history decides the outcome automatically:
//   - one version happens after the other: the newer version wins, no
//     conflict is reported;
//   - the versions are concurrent (written on both sides of a partition):
//     a genuine conflict, resolved deterministically by last-write-wins and
//     reported so the discarded version remains auditable.
//
// Running Reconcile in both directions makes both replicas converge to the
// same state; running it again is a no-op.
func (k *KVS) Reconcile(items <-chan *VersionedKeyValue) []*Conflict {
	start := time.Now()

	conflicts := make([]*Conflict, 0)
	mergedCount := uint64(0)
	conflictCount := uint64(0)

	for item := range items {
		if item == nil {
			continue
		}

		local, err := k.GetVersioned(item.Key)
		if err != nil && err != errors.ErrNotFound {
			k.logger.Error("failed to get local version", zap.String("key", item.Key), zap.Error(err))
			continue
		}

		var winner *VersionedValue
		conflict := false
		if local == nil {
			winner = item.Value
		} else {
			winner, conflict = resolve(local, item.Value)
		}

		if conflict {
			conflicts = append(conflicts, &Conflict{
				Key:         item.Key,
				LocalValue:  local.Value,
				RemoteValue: item.Value.Value,
				WinnerValue: winner.Value,
				LocalClock:  local.Clock,
				RemoteClock: item.Value.Clock,
			})
			conflictCount = conflictCount + 1
			k.logger.Warn("conflict detected and resolved",
				zap.String("key", item.Key),
				zap.String("local_clock", local.Clock.String()),
				zap.String("remote_clock", item.Value.Clock.String()),
				zap.String("winner_node", winner.NodeID))
		}

		if local == nil || winner != local {
			stored, err := encodeVersioned(winner)
			if err != nil {
				k.logger.Error("failed to encode versioned value", zap.String("key", item.Key), zap.Error(err))
				continue
			}
			if err := k.SetRaw(item.Key, stored); err != nil {
				k.logger.Error("failed to store reconciled value", zap.String("key", item.Key), zap.Error(err))
				continue
			}
			mergedCount = mergedCount + 1
		}
	}

	k.logger.Info("finished to reconcile items",
		zap.Uint64("merged", mergedCount),
		zap.Uint64("conflicts", conflictCount),
		zap.Float64("time", float64(time.Since(start))/float64(time.Second)))

	return conflicts
}
