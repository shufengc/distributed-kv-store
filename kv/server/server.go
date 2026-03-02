package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	resp := &kvrpcpb.GetResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.Version)

	lock, err := txn.GetLock(req.Key)
	if err != nil {
		return nil, err
	}
	if lock != nil && lock.Ts <= req.Version {
		resp.Error = &kvrpcpb.KeyError{Locked: lock.Info(req.Key)}
		return resp, nil
	}

	value, err := txn.GetValue(req.Key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		resp.NotFound = true
	} else {
		resp.Value = value
	}
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	resp := &kvrpcpb.PrewriteResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	keys := make([][]byte, len(req.Mutations))
	for i, mut := range req.Mutations {
		keys[i] = mut.Key
	}
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	server.Latches.Validate(txn, keys)

	var keyErrors []*kvrpcpb.KeyError
	for _, mut := range req.Mutations {
		write, commitTS, err := txn.MostRecentWrite(mut.Key)
		if err != nil {
			return nil, err
		}
		if write != nil && commitTS > req.StartVersion {
			keyErrors = append(keyErrors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: commitTS,
					Key:        mut.Key,
					Primary:    req.PrimaryLock,
				},
			})
			continue
		}

		lock, err := txn.GetLock(mut.Key)
		if err != nil {
			return nil, err
		}
		if lock != nil {
			keyErrors = append(keyErrors, &kvrpcpb.KeyError{Locked: lock.Info(mut.Key)})
			continue
		}

		switch mut.Op {
		case kvrpcpb.Op_Put:
			txn.PutValue(mut.Key, mut.Value)
		case kvrpcpb.Op_Del:
			txn.DeleteValue(mut.Key)
		}
		txn.PutLock(mut.Key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    mvcc.WriteKindFromProto(mut.Op),
		})
	}

	if len(keyErrors) > 0 {
		resp.Errors = keyErrors
		return resp, nil
	}

	if err = server.storage.Write(req.Context, txn.Writes()); err != nil {
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	resp := &kvrpcpb.CommitResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	server.Latches.Validate(txn, req.Keys)

	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock == nil {
			write, _, err := txn.CurrentWrite(key)
			if err != nil {
				return nil, err
			}
			if write != nil && write.Kind == mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{Abort: "true"}
				return resp, nil
			}
			// write == nil: prewrite was lost, treat as no-op.
			// write != nil (non-rollback): already committed, skip idempotently.
			continue
		}
		if lock.Ts != req.StartVersion {
			resp.Error = &kvrpcpb.KeyError{Retryable: "true"}
			return resp, nil
		}
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{StartTS: req.StartVersion, Kind: lock.Kind})
		txn.DeleteLock(key)
	}

	if err = server.storage.Write(req.Context, txn.Writes()); err != nil {
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	resp := &kvrpcpb.ScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()

	var pairs []*kvrpcpb.KvPair
	for i := uint32(0); i < req.Limit; i++ {
		key, value, err := scanner.Next()
		if key == nil {
			break
		}
		if err != nil {
			pairs = append(pairs, &kvrpcpb.KvPair{Error: &kvrpcpb.KeyError{}, Key: key})
			break
		}
		pairs = append(pairs, &kvrpcpb.KvPair{Key: key, Value: value})
	}
	resp.Pairs = pairs
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	resp := &kvrpcpb.CheckTxnStatusResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	primaryKey := req.PrimaryKey
	server.Latches.WaitForLatches([][]byte{primaryKey})
	defer server.Latches.ReleaseLatches([][]byte{primaryKey})

	txn := mvcc.NewMvccTxn(reader, req.LockTs)
	server.Latches.Validate(txn, [][]byte{primaryKey})

	lock, err := txn.GetLock(primaryKey)
	if err != nil {
		return nil, err
	}

	if lock == nil {
		// No lock — check if already committed or rolled back.
		write, commitTS, err := txn.CurrentWrite(primaryKey)
		if err != nil {
			return nil, err
		}
		if write != nil && write.Kind != mvcc.WriteKindRollback {
			// Already committed.
			resp.CommitVersion = commitTS
			resp.Action = kvrpcpb.Action_NoAction
			return resp, nil
		}
		if write != nil && write.Kind == mvcc.WriteKindRollback {
			// Already rolled back.
			resp.Action = kvrpcpb.Action_NoAction
			return resp, nil
		}
		// No record at all — write a rollback tombstone.
		txn.PutWrite(primaryKey, req.LockTs, &mvcc.Write{StartTS: req.LockTs, Kind: mvcc.WriteKindRollback})
		if err = server.storage.Write(req.Context, txn.Writes()); err != nil {
			return nil, err
		}
		resp.Action = kvrpcpb.Action_LockNotExistRollback
		return resp, nil
	}

	// Lock exists — check TTL.
	if mvcc.PhysicalTime(lock.Ts)+lock.Ttl < mvcc.PhysicalTime(req.CurrentTs) {
		// TTL expired — roll back.
		txn.DeleteLock(primaryKey)
		if lock.Kind == mvcc.WriteKindPut {
			txn.DeleteValue(primaryKey)
		}
		txn.PutWrite(primaryKey, req.LockTs, &mvcc.Write{StartTS: req.LockTs, Kind: mvcc.WriteKindRollback})
		if err = server.storage.Write(req.Context, txn.Writes()); err != nil {
			return nil, err
		}
		resp.Action = kvrpcpb.Action_TTLExpireRollback
		return resp, nil
	}

	// Lock is still alive.
	resp.LockTtl = lock.Ttl
	resp.Action = kvrpcpb.Action_NoAction
	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	resp := &kvrpcpb.BatchRollbackResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	server.Latches.Validate(txn, req.Keys)

	for _, key := range req.Keys {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		if write != nil {
			if write.Kind != mvcc.WriteKindRollback {
				// Already committed — abort.
				resp.Error = &kvrpcpb.KeyError{Abort: "true"}
				return resp, nil
			}
			// Already rolled back — skip idempotently.
			continue
		}

		// No existing record — perform the rollback.
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Ts == req.StartVersion {
			txn.DeleteLock(key)
			if lock.Kind == mvcc.WriteKindPut {
				txn.DeleteValue(key)
			}
		}
		txn.PutWrite(key, req.StartVersion, &mvcc.Write{StartTS: req.StartVersion, Kind: mvcc.WriteKindRollback})
	}

	if err = server.storage.Write(req.Context, txn.Writes()); err != nil {
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	resp := &kvrpcpb.ResolveLockResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	pairs, err := mvcc.AllLocksForTxn(txn)
	if err != nil {
		return nil, err
	}

	keys := make([][]byte, len(pairs))
	for i, pair := range pairs {
		keys[i] = pair.Key
	}

	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)
	server.Latches.Validate(txn, keys)

	for _, pair := range pairs {
		key, lock := pair.Key, pair.Lock
		if req.CommitVersion == 0 {
			// Rollback.
			txn.DeleteLock(key)
			if lock.Kind == mvcc.WriteKindPut {
				txn.DeleteValue(key)
			}
			txn.PutWrite(key, req.StartVersion, &mvcc.Write{StartTS: req.StartVersion, Kind: mvcc.WriteKindRollback})
		} else {
			// Commit.
			txn.PutWrite(key, req.CommitVersion, &mvcc.Write{StartTS: req.StartVersion, Kind: lock.Kind})
			txn.DeleteLock(key)
		}
	}

	if err = server.storage.Write(req.Context, txn.Writes()); err != nil {
		return nil, err
	}
	return resp, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
