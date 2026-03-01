package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
//
// Internally it wraps a single badger database. Column families are simulated by the engine_util
// package, which prefixes every key with the column family name (e.g. "default_mykey").
type StandAloneStorage struct {
	badgerDB        *badger.DB
	dbDirectoryPath string
}

// NewStandAloneStorage creates a new StandAloneStorage using the directory path found in the
// supplied configuration. The badger database is NOT opened here; that is deferred to Start().
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{
		dbDirectoryPath: conf.DBPath,
	}
}

// Start opens the underlying badger database on disk. It must be called before any reads or writes.
func (s *StandAloneStorage) Start() error {
	s.badgerDB = engine_util.CreateDB(s.dbDirectoryPath, false)
	return nil
}

// Stop closes the underlying badger database, flushing any pending writes and releasing file locks.
func (s *StandAloneStorage) Stop() error {
	return s.badgerDB.Close()
}

// Reader returns a StorageReader that provides a consistent snapshot view of the database at the
// moment of the call. The caller is responsible for closing the reader when done.
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// NewTransaction(false) opens a read-only transaction. Badger guarantees that all reads
	// through this transaction see a consistent snapshot of the data.
	readOnlyTransaction := s.badgerDB.NewTransaction(false)
	return &StandAloneStorageReader{
		badgerReadTransaction: readOnlyTransaction,
	}, nil
}

// Write applies a batch of Put and Delete modifications to the database atomically.
func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	writeBatch := new(engine_util.WriteBatch)

	for _, modification := range batch {
		switch operationData := modification.Data.(type) {
		case storage.Put:
			writeBatch.SetCF(operationData.Cf, operationData.Key, operationData.Value)
		case storage.Delete:
			writeBatch.DeleteCF(operationData.Cf, operationData.Key)
		}
	}

	return writeBatch.WriteToDB(s.badgerDB)
}

// StandAloneStorageReader provides point-get and iteration over a consistent snapshot of the
// database. It is backed by a single read-only badger transaction.
type StandAloneStorageReader struct {
	badgerReadTransaction *badger.Txn
}

// GetCF retrieves the value for the given key within the specified column family. If the key does
// not exist, (nil, nil) is returned — absence of a key is not treated as an error at this layer.
func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCFFromTxn(r.badgerReadTransaction, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return value, err
}

// IterCF returns an iterator positioned before the first key in the given column family. The
// caller must close the iterator before closing the reader.
func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.badgerReadTransaction)
}

// Close discards the underlying read transaction, releasing the snapshot and any associated memory.
// All iterators obtained from this reader must be closed before calling Close.
func (r *StandAloneStorageReader) Close() {
	r.badgerReadTransaction.Discard()
}
