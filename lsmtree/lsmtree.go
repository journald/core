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

	return &LSMTree{
		Threshold: threshold,
		C0:        c0,
		C1:        c1,
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

	return nil
}

func (t *LSMTree) Get(key []byte) ([]byte, error) {
	value, err := t.C0.Get(key)
	if err == nil {
		return value, nil
	}

	return t.C1.Get(key)
}

func (t *LSMTree) Scan(from []byte, fn func(key, data []byte)) error {
	err := t.C1.Scan(from, fn)
	if err != nil {
		return t.C0.Scan(from, fn)
	} else {
		return t.C0.ScanAll(fn)
	}
	return fmt.Errorf("key %s not found", from)
}

func (t *LSMTree) ScanAll(fn func(key, data []byte)) error {
	err := t.C1.ScanAll(fn)
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
	return t.C1.Close()
}

func (t *LSMTree) String() string {
	lines := []string{}
	t.ScanAll(func(k, v []byte) {
		lines = append(lines, fmt.Sprintf("%s | %s", k, v))
	})

	return strings.Join(lines, "\n")
}
