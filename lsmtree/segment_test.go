package lsmtree

import (
	"bytes"
	"io"
	"io/ioutil"
	"testing"
)

func TestMerge(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "data")
	if err != nil {
		t.Error(err)
	}

	older, err := NewSegment(tempDir)
	if err != nil {
		t.Error(err)
	}

	tempDir, err = ioutil.TempDir("", "data")
	if err != nil {
		t.Error(err)
	}
	newer, err := NewSegment(tempDir)
	if err != nil {
		t.Error(err)
	}

	// populate data
	data := []struct {
		key   []byte
		value []byte
	}{
		{
			[]byte("keyA"),
			[]byte("valueA"),
		},
		{
			[]byte("keyB"),
			[]byte("valueB"),
		},
		{
			[]byte("keyC"),
			[]byte("valueC"),
		},
	}

	for _, datum := range data {
		err = older.Put(datum.key, datum.value)
		if err != nil {
			t.Error(err)
		}
	}

	data = []struct {
		key   []byte
		value []byte
	}{
		{
			[]byte("keyZ"),
			[]byte("valueZ"),
		},
	}

	for _, datum := range data {
		err = newer.Put(datum.key, datum.value)
		if err != nil {
			t.Error(err)
		}
	}

	err = older.Merge(newer)
	if err != nil {
		t.Error(err)
	}

	size, err := newer.DataFile.Seek(0, io.SeekEnd)
	if size != 0 {
		t.Errorf("Expected Segment merge process to wipe newer segment data file but the file has size %d bytes", size)
	}

	found, _ := newer.SSTable.Index.Search([]byte("keyZ"))
	if found {
		t.Errorf("Expected Segment SSTable in memory index to be cleaned.")
	}

	value, err := older.Get([]byte("keyZ"))
	if err != nil || bytes.Compare(value, []byte("valueZ")) != 0 {
		t.Errorf("Expected to find newer keys inside older index after merge")
	}
}
