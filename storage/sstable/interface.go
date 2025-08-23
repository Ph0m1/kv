package sstable

type Iterator interface {
	Next() bool
	Key() []byte
	Value() []byte
	IsTombstone() bool
}
