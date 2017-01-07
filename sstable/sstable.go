package sstable

import (
	"fmt"
	"io"

	"github.com/journald/btree"
)

type SSTable struct {
	Index *btree.Tree
	Data  io.ReadWriteSeeker
}

func New(data io.ReadWriteSeeker) SSTable {
	return SSTable{
		Index: btree.New(),
		Data:  data,
	}
}

func (t SSTable) Load() error {
	_, err := t.Data.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	for {
		entry, err := ReadDataEntry(t.Data)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		t.Index.Insert(entry.Key, entry.Offset)
	}

	return nil
}

func Load(data io.ReadWriteSeeker) (SSTable, error) {
	t := New(data)
	return t, t.Load()
}

func (t SSTable) Put(key, value []byte) error {
	offset, err := t.Data.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	t.Index.Insert(key, offset)

	entry := NewDataEntry(key, value)

	return entry.Write(t.Data)
}

func (t SSTable) Get(key []byte) ([]byte, error) {
	ok, offset := t.Index.Search(key)
	if !ok {
		return nil, fmt.Errorf("key '%s' not found", key)
	}

	_, err := t.Data.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}

	entry, err := ReadDataEntry(t.Data)
	if err != nil {
		return nil, err
	}

	return entry.Data, nil
}

func (t SSTable) Scan(from []byte, fn func(key, data []byte)) error {
	value, err := t.Get(from)
	if err != nil {
		return err
	}

	fn(from, value)

	for entry, err := ReadDataEntry(t.Data); err == nil; entry, err = ReadDataEntry(t.Data) {
		fn(entry.Key, entry.Data)
	}

	if err == io.EOF {
		return nil
	} else {
		return err
	}
}

func (t SSTable) ScanAll(fn func(key, data []byte)) error {
	_, err := t.Data.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	for entry, err := ReadDataEntry(t.Data); err == nil; entry, err = ReadDataEntry(t.Data) {
		fn(entry.Key, entry.Data)
	}

	if err == io.EOF {
		return nil
	} else {
		return err
	}
}

func (t SSTable) Walk(fn btree.WalkerFunc) {
	t.Index.Walk(fn)
}

func (older SSTable) Merge(newer SSTable) error {
	// Ensure we are copying from start
	nbytes, err := older.Data.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	_, err = newer.Data.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	// Merge data
	_, err = io.Copy(older.Data, newer.Data)
	if err != nil {
		return err
	}

	newer.Walk(func(key []byte, offset int64) {
		older.Index.Insert(key, offset+nbytes)
	})

	return nil
}

func (t SSTable) Size() int64 {
	if t.Index == nil {
		return int64(0)
	}

	var count int64
	t.Walk(func(_ []byte, _ int64) {
		count += 1
	})

	return count
}

func (t SSTable) Keys() [][]byte {
	return t.Index.Keys()
}
