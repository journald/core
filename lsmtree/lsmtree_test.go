package lsmtree

import (
	"bytes"
	"io/ioutil"
	"reflect"
	"testing"
)

func TestGetPut(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "data")
	if err != nil {
		t.Error(err)
	}

	tree, err := New(1, tempDir)
	if err != nil {
		t.Error(err)
	}

	// 10 letters starting from ASCII 'A': 65
	for i := 65; i <= 75; i++ {
		err = tree.Put(append([]byte("key"), byte(i)), []byte{byte(i)})
		if err != nil {
			t.Error(err)
		}
	}

	for i := 65; i <= 75; i++ {
		expected := []byte{byte(i)}
		key := append([]byte("key"), byte(i))

		actual, err := tree.Get(key)
		if err != nil {
			t.Error(err)
		}

		if bytes.Compare(expected, actual) != 0 {
			t.Errorf("Expected to find the right value at %s.\nExpected: %s\nGot:      %s", key, expected, actual)
		}
	}
}

func TestPutMergeC0IntoC1AfterThreshold(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "data")
	if err != nil {
		t.Error(err)
	}

	tree, err := New(5, tempDir)
	if err != nil {
		t.Error(err)
	}

	// 10 letters starting from ASCII 'A': 65
	for i := 65; i <= 75; i++ {
		err = tree.Put(append([]byte("key"), byte(i)), []byte{byte(i)})
		if err != nil {
			t.Error(err)
		}
	}

	if tree.C0.Size() != 1 {
		t.Errorf("Given inserted data, C0 level should have exactly %d elements. It has %d", 1, tree.C0.Size())
	}

	if tree.C1.Size() != 10 {
		t.Errorf("Given inserted data, C1 level should have exactly %d elements. It has %d", 10, tree.C1.Size())
	}

	expected := [][]byte{
		[]byte("keyK"),
	}
	actual := tree.C0.SSTable.Keys()
	for i, k := range expected {
		if bytes.Compare(k, actual[i]) != 0 {
			t.Errorf("Expected C0 keys to be %v, but got %v", ByteSliceSliceToStringSlice(expected), ByteSliceSliceToStringSlice(actual))
		}
	}

	expected = [][]byte{
		[]byte("keyA"),
		[]byte("keyB"),
		[]byte("keyC"),
		[]byte("keyD"),
		[]byte("keyE"),
		[]byte("keyF"),
		[]byte("keyG"),
		[]byte("keyH"),
		[]byte("keyI"),
		[]byte("keyJ"),
	}
	actual = tree.C1.SSTable.Keys()
	for i, k := range expected {
		if bytes.Compare(k, actual[i]) != 0 {
			t.Errorf("Expected C1 keys to be %v, but got %v", ByteSliceSliceToStringSlice(expected), ByteSliceSliceToStringSlice(actual))
		}
	}
}

func TestPutMergeC1IntoC2AfterThreshold(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "data")
	if err != nil {
		t.Error(err)
	}

	tree, err := New(2, tempDir)
	if err != nil {
		t.Error(err)
	}

	// 25 letters starting from ASCII 'A': 65
	for i := 65; i <= 89; i++ {
		err = tree.Put(append([]byte("key"), byte(i)), []byte{byte(i)})
		if err != nil {
			t.Error(err)
		}
	}

	if tree.C0.Size() != 1 {
		t.Errorf("Given inserted data, C0 level should have exactly %d elements. It has %d", 1, tree.C0.Size())
	}

	if tree.C1.Size() != 4 {
		t.Errorf("Given inserted data, C1 level should have exactly %d elements. It has %d", 10, tree.C1.Size())
	}

	if tree.C2.Size() != 20 {
		t.Errorf("Given inserted data, C1 level should have exactly %d elements. It has %d", 10, tree.C1.Size())
	}

	expected := [][]byte{
		[]byte("keyY"),
	}
	actual := tree.C0.SSTable.Keys()
	for i, k := range expected {
		if bytes.Compare(k, actual[i]) != 0 {
			t.Errorf("Expected C0 keys to be %v, but got %v", ByteSliceSliceToStringSlice(expected), ByteSliceSliceToStringSlice(actual))
		}
	}

	expected = [][]byte{
		[]byte("keyU"),
		[]byte("keyV"),
		[]byte("keyW"),
		[]byte("keyX"),
	}
	actual = tree.C1.SSTable.Keys()
	for i, k := range expected {
		if bytes.Compare(k, actual[i]) != 0 {
			t.Errorf("Expected C1 keys to be %v, but got %v", ByteSliceSliceToStringSlice(expected), ByteSliceSliceToStringSlice(actual))
		}
	}

	expected = [][]byte{
		[]byte("keyA"),
		[]byte("keyB"),
		[]byte("keyC"),
		[]byte("keyD"),
		[]byte("keyE"),
		[]byte("keyF"),
		[]byte("keyG"),
		[]byte("keyH"),
		[]byte("keyI"),
		[]byte("keyJ"),
		[]byte("keyK"),
		[]byte("keyL"),
		[]byte("keyM"),
		[]byte("keyN"),
		[]byte("keyO"),
		[]byte("keyP"),
		[]byte("keyQ"),
		[]byte("keyR"),
		[]byte("keyS"),
		[]byte("keyT"),
	}
	actual = tree.C2.SSTable.Keys()
	for i, k := range expected {
		if bytes.Compare(k, actual[i]) != 0 {
			t.Errorf("Expected C2 keys to be %v, but got %v", ByteSliceSliceToStringSlice(expected), ByteSliceSliceToStringSlice(actual))
		}
	}
}

func TestScan(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "data")
	if err != nil {
		t.Error(err)
	}

	tree, err := New(2, tempDir)
	if err != nil {
		t.Error(err)
	}

	// 25 letters starting from ASCII 'A': 65
	for i := 65; i <= 89; i++ {
		err = tree.Put(append([]byte("key"), byte(i)), []byte{byte(i)})
		if err != nil {
			t.Error(err)
		}
	}

	expected := map[string]string{
		"keyT": "T",
		"keyU": "U",
		"keyV": "V",
		"keyW": "W",
		"keyX": "X",
		"keyY": "Y",
	}
	actual, err := CaptureScan(tree, []byte("keyT"))
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expected scan to look like %v, but got %v", expected, actual)
	}
}

func TestScanAll(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "data")
	if err != nil {
		t.Error(err)
	}

	tree, err := New(5, tempDir)
	if err != nil {
		t.Error(err)
	}

	// 10 letters starting from ASCII 'A': 65
	for i := 65; i <= 75; i++ {
		err = tree.Put(append([]byte("key"), byte(i)), []byte{byte(i)})
		if err != nil {
			t.Error(err)
		}
	}

	expected := map[string]string{
		"keyA": "A",
		"keyB": "B",
		"keyC": "C",
		"keyD": "D",
		"keyE": "E",
		"keyF": "F",
		"keyG": "G",
		"keyH": "H",
		"keyI": "I",
		"keyJ": "J",
		"keyK": "K",
	}
	actual, err := CaptureScanAll(tree)
	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expected scan to look like be %v, but got %v", expected, actual)
	}
}

func TestString(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "data")
	if err != nil {
		t.Error(err)
	}

	tree, err := New(5, tempDir)
	if err != nil {
		t.Error(err)
	}

	// 10 letters starting from ASCII 'A': 65
	for i := 65; i <= 75; i++ {
		err = tree.Put(append([]byte("key"), byte(i)), []byte{byte(i)})
		if err != nil {
			t.Error(err)
		}
	}

	expected := `keyA | A
keyB | B
keyC | C
keyD | D
keyE | E
keyF | F
keyG | G
keyH | H
keyI | I
keyJ | J
keyK | K`
	actual := tree.String()
	if actual != expected {
		t.Errorf("Expected table to look like\nExpected\n%v\n\nGot\n%v", expected, actual)
	}
}

func ByteSliceSliceToStringSlice(slice [][]byte) []string {
	var result []string
	for _, v := range slice {
		result = append(result, string(v))
	}
	return result
}

func CaptureScanAll(tree *LSMTree) (map[string]string, error) {
	values := make(map[string]string)

	tree.ScanAll(func(key, value []byte) {
		values[string(key)] = string(value)
	})

	return values, nil
}

func CaptureScan(tree *LSMTree, from []byte) (map[string]string, error) {
	values := make(map[string]string)

	err := tree.Scan(from, func(key, value []byte) {
		values[string(key)] = string(value)
	})

	return values, err
}
