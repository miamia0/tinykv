package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
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

// Raw API.
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		ret := &kvrpcpb.RawGetResponse{
			Error:    err.Error(),
			NotFound: true,
		}
		return ret, err
	}
	value, err := reader.GetCF(req.GetCf(), req.GetKey())
	isNotFound := value == nil
	_err := ""
	if err != nil {
		_err = err.Error()
	}
	ret := &kvrpcpb.RawGetResponse{
		Error:    _err,
		Value:    value,
		NotFound: isNotFound,
	}
	return ret, nil
}

func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	err := server.storage.Write(
		req.GetContext(),
		[]storage.Modify{
			{
				Data: storage.Put{
					Key:   req.GetKey(),
					Value: req.GetValue(),
					Cf:    req.GetCf(),
				},
			},
		})
	_err := ""
	if err != nil {
		_err = err.Error()
	}
	ret := &kvrpcpb.RawPutResponse{
		Error: _err,
	}
	return ret, nil
}

func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	err := server.storage.Write(
		req.GetContext(),
		[]storage.Modify{
			{
				Data: storage.Delete{
					Key: req.GetKey(),
					Cf:  req.GetCf(),
				},
			},
		})
	_err := ""

	if err != nil {
		_err = err.Error()
	}
	ret := &kvrpcpb.RawDeleteResponse{
		Error: _err,
	}
	return ret, err
}

func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		ret := &kvrpcpb.RawScanResponse{
			Error: err.Error(),
		}
		return ret, err
	}

	_iter := reader.IterCF(req.GetCf())
	pairs := []*kvrpcpb.KvPair{}
	_iter.Seek(req.GetStartKey())
	var i uint32 = 0
	for ; _iter.Valid() && i < req.GetLimit(); i = i + 1 {
		item := _iter.Item()
		_key := item.Key()
		_value, _ := item.Value()
		pairs = append(pairs, &kvrpcpb.KvPair{
			Error: nil,
			Key:   _key,
			Value: _value,
		})
		_iter.Next()
	}
	ret := &kvrpcpb.RawScanResponse{
		Kvs: pairs,
	}
	reader.Close()
	return ret, nil
}

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
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.GetContext())
	txn := mvcc.NewMvccTxn(reader, req.GetVersion())
	defer txn.Reader.Close()
	resp := &kvrpcpb.GetResponse{}
	lock, err := txn.GetLock(req.Key)
	if lock != nil && lock.IsLockedFor(req.Key, req.Version, resp) {
		return resp, nil
	}
	value, err := txn.GetValue(req.Key)
	if err != nil {
		return nil, err
	}
	resp.Value = value

	if value == nil {
		resp.NotFound = true
	}
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	keys := make([][]byte, 0)
	for _, mt := range req.Mutations {
		keys = append(keys, mt.Key)
	}
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)
	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())
	defer txn.Reader.Close()
	resp := &kvrpcpb.PrewriteResponse{}
	for _, mt := range req.Mutations {
		switch mt.Op {
		case kvrpcpb.Op_Put:
			txn.PutValue(mt.Key, mt.Value)
		case kvrpcpb.Op_Del:
			txn.DeleteValue(mt.Key)
		case kvrpcpb.Op_Rollback:
			txn.PutValue(mt.Key, mt.Value)
		}
		_, ts, err := txn.MostRecentWrite(mt.Key)
		if err != nil || ts > req.StartVersion {
			resp.Errors = append(resp.Errors,
				&kvrpcpb.KeyError{
					Conflict: &kvrpcpb.WriteConflict{
						StartTs:    req.StartVersion,
						ConflictTs: ts,
						Key:        mt.Key,
						Primary:    req.PrimaryLock,
					},
				},
			)

			return resp, err
		}
		lock, _ := txn.GetLock(mt.Key)
		Err := &kvrpcpb.GetResponse{}
		if lock != nil && lock.IsLockedFor(mt.Key, req.GetStartVersion(), Err) {
			resp.Errors = append(resp.Errors, Err.Error)
			return resp, nil
		}
		txn.PutLock(mt.Key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    mvcc.WriteKindFromProto(mt.Op),
		})
	}
	server.storage.Write(req.GetContext(), txn.Writes())

	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	server.Latches.WaitForLatches(req.GetKeys())
	defer server.Latches.ReleaseLatches(req.GetKeys())
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())
	defer txn.Reader.Close()
	resp := &kvrpcpb.CommitResponse{}
	for _, key := range req.Keys {
		lock, _ := txn.GetLock(key)
		if lock == nil {
			return resp, nil
		}
		if lock.Ts < req.StartVersion {
			resp.Error = &kvrpcpb.KeyError{
				Retryable: string(key),
			}
			return resp, nil
		}
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{StartTS: req.StartVersion, Kind: lock.Kind})
		txn.DeleteLock(key)
	}
	server.storage.Write(req.GetContext(), txn.Writes())

	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	reader, _ := server.storage.Reader(req.GetContext())
	scanner := mvcc.NewScanner(req.StartKey, mvcc.NewMvccTxn(reader, req.GetVersion()))
	defer scanner.Close()
	pairs := []*kvrpcpb.KvPair{}
	resp := &kvrpcpb.ScanResponse{}

	var i uint32 = 0
	for ; scanner.Valid() && i < req.GetLimit(); i = i + 1 {
		key, value, _ := scanner.Next()
		pairs = append(pairs, &kvrpcpb.KvPair{
			Error: nil,
			Key:   key,
			Value: value,
		})
	}
	resp.Pairs = pairs
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	server.Latches.WaitForLatches([][]byte{req.PrimaryKey})
	defer server.Latches.ReleaseLatches([][]byte{req.PrimaryKey})

	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.GetLockTs())
	defer txn.Reader.Close()
	resp := &kvrpcpb.CheckTxnStatusResponse{}
	resp.Action = kvrpcpb.Action_NoAction
	lock, _ := txn.GetLock(req.PrimaryKey)
	write, ts, _ := txn.CurrentWrite(req.PrimaryKey)

	if ts != 0 && write.Kind == mvcc.WriteKindRollback {
		return resp, nil
	} else if ts != 0 && write.Kind != mvcc.WriteKindRollback {
		resp.CommitVersion = write.StartTS
		return resp, nil
	}
	if lock == nil {
		resp.Action = kvrpcpb.Action_LockNotExistRollback
	} else {
		resp.LockTtl = lock.Ttl

		if lock.Ttl+mvcc.PhysicalTime(lock.Ts) <= mvcc.PhysicalTime(req.GetCurrentTs()) {
			txn.DeleteLock(req.PrimaryKey)
			txn.DeleteValue(req.PrimaryKey)
			resp.Action = kvrpcpb.Action_TTLExpireRollback

		} else {
			return resp, nil
		}
	}
	txn.PutWrite(req.PrimaryKey,
		req.LockTs,
		&mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		},
	)
	server.storage.Write(req.GetContext(), txn.Writes())
	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())
	defer txn.Reader.Close()
	resp := &kvrpcpb.BatchRollbackResponse{}
	for _, key := range req.Keys {
		lock, _ := txn.GetLock(key)

		if lock != nil && lock.Ts == req.StartVersion {
			txn.DeleteLock(key)
			txn.DeleteValue(key)
		}
		write, ts, _ := txn.CurrentWrite(key)

		if ts != 0 && write.Kind != mvcc.WriteKindRollback {
			resp.Error = &kvrpcpb.KeyError{Abort: string(key)}
			continue
		} else if ts != 0 && write.Kind == mvcc.WriteKindRollback {
			continue
		}

		txn.PutWrite(key,
			req.StartVersion,
			&mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    mvcc.WriteKindRollback,
			},
		)
	}
	server.storage.Write(req.GetContext(), txn.Writes())
	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())
	defer txn.Reader.Close()
	resp := &kvrpcpb.ResolveLockResponse{}
	iter := reader.IterCF(engine_util.CfLock)
	defer iter.Close()
	keyRB := make([][]byte, 0)
	keyComm := make([][]byte, 0)
	for ; iter.Valid(); iter.Next() {
		key := iter.Item().Key()
		lockbytes, _ := iter.Item().Value()
		lock, _ := mvcc.ParseLock(lockbytes)
		if lock.Ts == req.StartVersion {
			if req.CommitVersion < req.StartVersion {
				keyRB = append(keyRB, key)
			} else {
				keyComm = append(keyComm, key)
			}
		}
	}
	server.KvBatchRollback(nil, &kvrpcpb.BatchRollbackRequest{
		StartVersion: req.StartVersion,
		Keys:         keyRB,
	})

	server.KvCommit(nil, &kvrpcpb.CommitRequest{
		StartVersion:  req.StartVersion,
		Keys:          keyComm,
		CommitVersion: req.CommitVersion,
	})
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
