package lsmtree

import (
	"fmt"
	"os"
	"path"
	"strings"
)

type LSMTree struct {
	Threshold int64
	C0        *Segment
	C1        *Segment
	C2        *Segment
}

func New(threshold int64, dataPath string) (*LSMTree, error) {
	c0path := path.Join(dataPath, "0")
	err := os.MkdirAll(c0path, 0755)
	if err != nil {
		return &LSMTree{}, err
	}
	c0, err := NewSegment(c0path)
	if err != nil {
		return &LSMTree{}, err
	}

	c1path := path.Join(dataPath, "1")
	err = os.MkdirAll(c1path, 0755)
	if err != nil {
		return &LSMTree{}, err
	}
	c1, err := NewSegment(c1path)
	if err != nil {
		return &LSMTree{}, err
	}

	c2path := path.Join(dataPath, "2")
	err = os.MkdirAll(c2path, 0755)
	if err != nil {
		return &LSMTree{}, err
	}
	c2, err := NewSegment(c2path)
	if err != nil {
		return &LSMTree{}, err
	}

	return &LSMTree{
		Threshold: threshold,
		C0:        c0,
		C1:        c1,
		C2:        c2,
	}, nil
}

func (t *LSMTree) Put(key, value []byte) error {
	err := t.C0.Put(key, value)
	if err != nil {
		return err
	}

	if t.C0.Size() >= t.Threshold {
		t.C1.Merge(t.C0)
	}

	if t.C1.Size() >= 10*t.Threshold {
		t.C2.Merge(t.C1)
	}

	return nil
}

func (t *LSMTree) Get(key []byte) ([]byte, error) {
	value, err := t.C0.Get(key)
	if err == nil {
		return value, nil
	}

	value, err = t.C1.Get(key)
	if err == nil {
		return value, nil
	}

	return t.C2.Get(key)
}

func (t *LSMTree) Scan(from []byte, fn func(key, data []byte)) error {
	// We start looking for 'from' key in the oldest C2 level
	err := t.C2.Scan(from, fn)
	if err != nil {
		// 'from' key is NOT in C2 "older" level, so we check if it is a slightly
		// newer C1 level
		err := t.C1.Scan(from, fn)
		if err != nil {
			// 'from' is not in C1 level neither, we look for it in C0
			return t.C0.Scan(from, fn)
		} else {
			// 'from' is in C1, we scan also the "newer" C0 level
			return t.C0.ScanAll(fn)
		}
	} else {
		// 'from' key is in C2 "older" level, so we scan all "newer" levels
		err := t.C1.ScanAll(fn)
		if err != nil {
			return err
		}

		return t.C0.ScanAll(fn)
	}
	return fmt.Errorf("key %s not found", from)
}

func (t *LSMTree) ScanAll(fn func(key, data []byte)) error {
	err := t.C2.ScanAll(fn)
	if err != nil {
		return err
	}

	err = t.C1.ScanAll(fn)
	if err != nil {
		return err
	}

	return t.C0.ScanAll(fn)
}

func (t *LSMTree) Close() error {
	err := t.C0.Close()
	if err != nil {
		return err
	}
	err = t.C1.Close()
	if err != nil {
		return err
	}
	return t.C2.Close()
}

func (t *LSMTree) String() string {
	lines := []string{}
	t.ScanAll(func(k, v []byte) {
		lines = append(lines, fmt.Sprintf("%s | %s", k, v))
	})

	return strings.Join(lines, "\n")
}
