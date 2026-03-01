package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}

	response := &kvrpcpb.RawGetResponse{}
	if value == nil {
		response.NotFound = true
	} else {
		response.Value = value
	}
	return response, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	modifyBatch := []storage.Modify{
		{
			Data: storage.Put{
				Cf:    req.Cf,
				Key:   req.Key,
				Value: req.Value,
			},
		},
	}

	err := server.storage.Write(req.Context, modifyBatch)
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	modifyBatch := []storage.Modify{
		{
			Data: storage.Delete{
				Cf:  req.Cf,
				Key: req.Key,
			},
		},
	}

	err := server.storage.Write(req.Context, modifyBatch)
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	// The iterator must be closed before the reader (which discards the underlying transaction).
	// Because defer runs in LIFO order, declaring the iterator defer after the reader defer
	// ensures the iterator is closed first.
	iterator := reader.IterCF(req.Cf)
	defer iterator.Close()

	var collectedPairs []*kvrpcpb.KvPair
	scanLimit := int(req.Limit)

	for iterator.Seek(req.StartKey); iterator.Valid() && len(collectedPairs) < scanLimit; iterator.Next() {
		currentItem := iterator.Item()

		// KeyCopy and ValueCopy allocate fresh byte slices so that the returned
		// data is safe to use after the iterator advances.
		keyCopy := currentItem.KeyCopy(nil)
		valueCopy, err := currentItem.ValueCopy(nil)
		if err != nil {
			return nil, err
		}

		collectedPairs = append(collectedPairs, &kvrpcpb.KvPair{
			Key:   keyCopy,
			Value: valueCopy,
		})
	}

	return &kvrpcpb.RawScanResponse{Kvs: collectedPairs}, nil
}
