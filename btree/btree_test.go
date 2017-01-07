package btree

import "testing"

func TestBtree(t *testing.T) {
	tree := New()

	tree.Insert([]byte("foo"), 1)
	tree.Insert([]byte("bar"), 2)
	tree.Insert([]byte("baz"), 3)

	ok, value := tree.Search([]byte("foo"))
	if !ok || value != 1 {
		t.Errorf("Expected to find value %v for key %s but found %v", 1, "foo", value)
	}

	ok, value = tree.Search([]byte("bar"))
	if !ok || value != 2 {
		t.Errorf("Expected to find value %v for key %s but found %v", 1, "bar", value)
	}

	ok, value = tree.Search([]byte("baz"))
	if !ok || value != 3 {
		t.Errorf("Expected to find value %v for key %s but found %v", 1, "baz", value)
	}

	values := make(map[string]int64)
	tree.Walk(func(key []byte, value int64) {
		values[string(key)] = value
	})

	value = values["foo"]
	if value != 1 {
		t.Errorf("Expected to find value %v for key %s but found %v", 1, "foo", value)
	}

	value = values["bar"]
	if value != 2 {
		t.Errorf("Expected to find value %v for key %s but found %v", 1, "bar", value)
	}

	value = values["baz"]
	if value != 3 {
		t.Errorf("Expected to find value %v for key %s but found %v", 1, "baz", value)
	}
}
