package sstable

import (
	"bytes"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestSSTable(t *testing.T) {
	data := `FOO | foo
	         BAR | bar`
	table, teardown, err := GenerateTable(data)
	if err != nil {
		t.Error(err)
	}
	defer teardown()

	value, err := table.Get([]byte("BAR"))
	if err != nil {
		t.Errorf("Expected to find the key BAR\n")
	}
	if bytes.Compare([]byte("bar"), value) != 0 {
		t.Errorf("Expected to read the appended value '%s' but got '%s'\n", "bar", value)
	}

	value, err = table.Get([]byte("FOO"))
	if err != nil {
		t.Errorf("Expected to find the key FOO\n")
	}
	if bytes.Compare([]byte("foo"), value) != 0 {
		t.Errorf("Expected to read the appended value '%s' but got '%s'\n", "bar", value)
	}

	value, err = table.Get([]byte("DAFUQ"))
	if err == nil {
		t.Errorf("Expected to NOT find the key DAFUQ inside the store\n")
	}
}

func TestMergeSSTable(t *testing.T) {
	data := `left-key-01 | left-data-01
	         left-key-02 | left-data-02`
	left, teardown, err := GenerateTable(data)
	if err != nil {
		t.Error(err)
	}
	defer teardown()

	data = `right-key-01 | right-data-01
	        right-key-02 | right-data-02`
	right, teardown, err := GenerateTable(data)
	if err != nil {
		t.Error(err)
	}
	defer teardown()

	err = left.Merge(right)
	if err != nil {
		t.Error(err)
	}

	tt := []struct {
		Key   []byte
		Value []byte
	}{
		{
			[]byte("left-key-01"),
			[]byte("left-data-01"),
		},
		{
			[]byte("left-key-02"),
			[]byte("left-data-02"),
		},
		{
			[]byte("right-key-01"),
			[]byte("right-data-01"),
		},
		{
			[]byte("right-key-02"),
			[]byte("right-data-02"),
		},
	}

	for _, example := range tt {
		value, _ := left.Get(example.Key)
		if bytes.Compare(value, example.Value) != 0 {
			t.Errorf("Expected to find '%s' at key '%s' but found '%s'", example.Value, example.Key, value)
		}
	}
}

func TestScan(t *testing.T) {
	data := `FOO | foo
	         BAR | bar
					 BAZ | baz`
	table, teardown, err := GenerateTable(data)
	if err != nil {
		t.Error(err)
	}
	defer teardown()

	values, err := CaptureScan(table, []byte("FOO"))
	if err != nil {
		t.Error(err)
	}
	expected := map[string]string{
		"FOO": "foo",
		"BAR": "bar",
		"BAZ": "baz",
	}

	if !reflect.DeepEqual(values, expected) {
		t.Errorf("Expected scan to yield correct keys.\nExpected: %v\nGot:      %v", expected, values)
	}

	values, err = CaptureScan(table, []byte("BAR"))
	if err != nil {
		t.Error(err)
	}
	expected = map[string]string{
		"BAR": "bar",
		"BAZ": "baz",
	}

	if !reflect.DeepEqual(values, expected) {
		t.Errorf("Expected scan to yield correct keys.\nExpected: %v\nGot:      %v", expected, values)
	}

	err = table.Scan([]byte("unknown-key"), func(key, value []byte) {})
	if err == nil {
		t.Errorf("Expected scan to return a key not found error")
	}
}

func TestScanAll(t *testing.T) {
	data := `FOO | foo
	         BAR | bar
					 BAZ | baz`

	table, teardown, err := GenerateTable(data)
	if err != nil {
		t.Error(err)
	}
	defer teardown()

	values, err := CaptureScanAll(table)
	if err != nil {
		t.Error(err)
	}
	expected := map[string]string{
		"FOO": "foo",
		"BAR": "bar",
		"BAZ": "baz",
	}

	if !reflect.DeepEqual(values, expected) {
		t.Errorf("Expected scan to yield correct keys.\nExpected: %v\nGot:      %v", expected, values)
	}
}

func TestSize(t *testing.T) {
	tt := []struct {
		Data string
		Size int64
	}{
		{
			"",
			0,
		},
		{
			`keyA | valueA
			 keyB | valueB
			 keyC | valueC
			 keyD | valueD`,
			4,
		},
		{
			`keyA | valueA
			 keyZ | valueZ`,
			2,
		},
	}

	for _, example := range tt {
		table, teardown, err := GenerateTable(example.Data)
		if err != nil {
			t.Error(err)
		}
		defer teardown()

		if table.Size() != example.Size {
			t.Errorf("Expected table size is wrong.\nExpected: %v\nGot:      %v", example.Size, table.Size())
		}
	}
}

func TestKeys(t *testing.T) {
	tt := []struct {
		Data string
		Keys [][]byte
	}{
		{
			"",
			[][]byte{},
		},
		{
			`keyA | valueA
			 keyB | valueB
			 keyC | valueC
			 keyD | valueD`,
			[][]byte{
				[]byte("keyA"),
				[]byte("keyB"),
				[]byte("keyC"),
				[]byte("keyD"),
			},
		},
		{
			`keyA | valueA
			 keyZ | valueZ`,
			[][]byte{
				[]byte("keyA"),
				[]byte("keyZ"),
			},
		},
	}

	for _, example := range tt {
		table, teardown, err := GenerateTable(example.Data)
		if err != nil {
			t.Error(err)
		}
		defer teardown()

		for i, key := range example.Keys {
			if bytes.Compare(table.Keys()[i], key) != 0 {
				t.Errorf("Expected table size is wrong.\nExpected: %v\nGot:      %v", key, table.Keys()[i])
			}
		}
	}
}

type TeardownFunc func()

func GenerateTable(data string) (SSTable, TeardownFunc, error) {
	file, err := ioutil.TempFile("", "data")
	if err != nil {
		return SSTable{}, nil, err
	}

	teardown := func() {
		os.Remove(file.Name())
	}

	table := New(file)

	lines := strings.Split(data, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)

		if line == "" {
			continue
		}

		cols := strings.Split(line, "|")
		key := []byte(strings.TrimSpace(cols[0]))
		value := []byte(strings.TrimSpace(cols[1]))

		err := table.Put(key, value)
		if err != nil {
			return table, teardown, err
		}
	}

	return table, teardown, nil
}

func CaptureScanAll(tree SSTable) (map[string]string, error) {
	values := make(map[string]string)

	tree.ScanAll(func(key, value []byte) {
		values[string(key)] = string(value)
	})

	return values, nil
}

func CaptureScan(tree SSTable, from []byte) (map[string]string, error) {
	values := make(map[string]string)

	err := tree.Scan(from, func(key, value []byte) {
		values[string(key)] = string(value)
	})

	return values, err
}
