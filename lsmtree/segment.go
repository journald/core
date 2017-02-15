package lsmtree

import (
	"io"
	"os"
	"path"

	"github.com/journald/btree"
	"github.com/journald/sstable"
)

type Segment struct {
	SSTable  sstable.SSTable
	DataFile *os.File
}

func NewSegment(dir string) (*Segment, error) {
	file, err := os.OpenFile(path.Join(dir, "data"), os.O_RDWR|os.O_CREATE, 0660)
	if err != nil {
		return &Segment{}, err
	}

	table, err := sstable.Load(file)
	if err != nil {
		return &Segment{}, err
	}

	return &Segment{
		DataFile: file,
		SSTable:  table,
	}, nil
}

func (s *Segment) Merge(newer *Segment) error {
	err := s.SSTable.Merge(newer.SSTable)
	if err != nil {
		return err
	}
	_, err = newer.DataFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	err = newer.DataFile.Truncate(0)
	if err != nil {
		return err
	}
	newer.SSTable = sstable.New(newer.DataFile)
	return nil
}

func (s *Segment) Close() error {
	return s.DataFile.Close()
}

func (s *Segment) Put(key, value []byte) error {
	return s.SSTable.Put(key, value)
}

func (s *Segment) Get(key []byte) ([]byte, error) {
	return s.SSTable.Get(key)
}

func (s *Segment) Scan(from []byte, fn func(key, data []byte)) error {
	return s.SSTable.Scan(from, fn)
}

func (s *Segment) ScanAll(fn func(key, data []byte)) error {
	return s.SSTable.ScanAll(fn)
}

func (s *Segment) Walk(fn btree.WalkerFunc) {
	s.SSTable.Index.Walk(fn)
}

func (s *Segment) Size() int64 {
	return s.SSTable.Size()
}
