package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	Txn    *MvccTxn
	iter   engine_util.DBIterator
	preKey []byte
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	scanner := &Scanner{
		Txn:    txn,
		iter:   txn.Reader.IterCF(engine_util.CfWrite),
		preKey: startKey,
	}
	scanner.iter.Seek(EncodeKey(startKey, txn.StartTS))
	if !scanner.iter.Valid() {
		scanner.preKey = nil
	} else {
		scanner.preKey = DecodeUserKey(scanner.iter.Item().Key())
	}
	return scanner
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}
func (scan *Scanner) Valid() bool {
	// Your Code Here (4C).
	return scan.preKey != nil
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {

	key := scan.preKey
	iter := scan.iter
	var value []byte
	value = nil
	isblock := false
	for iter.Seek(EncodeKey(key, scan.Txn.StartTS)); iter.Valid(); iter.Next() {
		writeKey := iter.Item().Key()
		writeValue, _ := iter.Item().Value()
		retWrite, _ := ParseWrite(writeValue)
		if !bytes.Equal(DecodeUserKey(writeKey), key) {
			key = DecodeUserKey(writeKey)
			isblock = false
		}
		if retWrite.Kind == WriteKindDelete {
			isblock = true
		} else if decodeTimestamp(writeKey) <= scan.Txn.StartTS && !isblock {
			key = DecodeUserKey(writeKey)
			value, _ = scan.Txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, retWrite.StartTS))
			break
		}
	}
	if value == nil || !iter.Valid() {
		scan.preKey = nil
		return nil, nil, nil
	}
	scan.iter.Next()
	for scan.iter.Valid() && bytes.Equal(DecodeUserKey(scan.iter.Item().Key()), key) {
		scan.iter.Next()
	}
	if scan.iter.Valid() {
		scan.preKey = DecodeUserKey(scan.iter.Item().Key())
	} else {
		scan.preKey = nil
	}
	return key, value, nil
}
