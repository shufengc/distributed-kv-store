package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	txn     *MvccTxn
	iter    engine_util.DBIterator
	nextKey []byte
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	return &Scanner{
		txn:     txn,
		iter:    txn.Reader.IterCF(engine_util.CfWrite),
		nextKey: startKey,
	}
}

func (scan *Scanner) Close() {
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	for {
		if scan.nextKey == nil {
			return nil, nil, nil
		}

		currentKey := scan.nextKey
		scan.nextKey = nil

		// Seek to the most recent write for currentKey at or before txn.StartTS.
		scan.iter.Seek(EncodeKey(currentKey, scan.txn.StartTS))

		var foundWrite *Write

		for ; scan.iter.Valid(); scan.iter.Next() {
			itemUserKey := DecodeUserKey(scan.iter.Item().Key())

			if !bytes.Equal(itemUserKey, currentKey) {
				// Crossed into the next user key; record it for the next Next() call.
				scan.nextKey = itemUserKey
				break
			}

			// Still on currentKey. If we already know the result, just keep
			// iterating to locate the next distinct key.
			if foundWrite != nil {
				continue
			}

			val, err := scan.iter.Item().Value()
			if err != nil {
				return nil, nil, err
			}
			write, err := ParseWrite(val)
			if err != nil {
				return nil, nil, err
			}

			switch write.Kind {
			case WriteKindPut:
				foundWrite = write
			case WriteKindDelete:
				// Mark as "resolved but deleted" with a sentinel so we skip.
				foundWrite = write
			case WriteKindRollback:
				// Skip rollback records; look at the next older version.
			}
		}

		if foundWrite == nil || foundWrite.Kind == WriteKindDelete {
			// No visible write, or the key was deleted — skip to the next key.
			if scan.nextKey == nil {
				return nil, nil, nil
			}
			continue
		}

		// foundWrite.Kind == WriteKindPut: fetch the actual value.
		value, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(currentKey, foundWrite.StartTS))
		if err != nil {
			return nil, nil, err
		}
		return currentKey, value, nil
	}
}
