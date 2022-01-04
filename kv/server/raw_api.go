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
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		return &kvrpcpb.RawGetResponse{
			Error: err.Error(),
		}, err
	}
	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return &kvrpcpb.RawGetResponse{
			Error: err.Error(),
		}, err
	}
	res := &kvrpcpb.RawGetResponse{
		Value: val,
	}
	if val == nil {
		res.NotFound = true
	}
	return res, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	batch := []storage.Modify{
		{
			Data: storage.Put{
				Cf:    req.Cf,
				Key:   req.Key,
				Value: req.Value,
			},
		},
	}
	err := server.storage.Write(req.Context, batch)
	res := &kvrpcpb.RawPutResponse{}
	if err != nil {
		res.Error = err.Error()
		return res, err
	}
	return res, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	batch := []storage.Modify{
		{
			Data: storage.Delete{
				Key: req.Key,
				Cf:  req.Cf,
			},
		},
	}
	err := server.storage.Write(req.Context, batch)
	res := &kvrpcpb.RawDeleteResponse{}
	if err != nil {
		res.Error = err.Error()
		return res, err
	}
	return res, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	res := &kvrpcpb.RawScanResponse{}
	if req.Limit == 0 {
		return res, nil
	}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		res.Error = err.Error()
		return res, err
	}
	iter := reader.IterCF(req.Cf)
	var cnt uint32 = 0
	pairs := make([]*kvrpcpb.KvPair, 0)
	for iter.Seek(req.StartKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		val, err := item.Value()
		if err != nil {
			res.Error = err.Error()
			return res, err
		}
		pairs = append(pairs, &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: val,
		})
		cnt++
		if cnt >= req.Limit {
			break
		}
	}
	res.Kvs = pairs
	return res, nil
}
