package btree

import "bytes"

type WalkerFunc func(key []byte, value int64)

type Tree struct {
	key   []byte
	value int64
	left  *Tree
	right *Tree
}

func New() *Tree {
	return &Tree{}
}

func (tree *Tree) Insert(key []byte, value int64) {
	if tree.key == nil {
		tree.key = key
		tree.value = value
		tree.right = New()
		tree.left = New()
	} else {
		if bytes.Compare(key, tree.key) <= 0 {
			tree.left.Insert(key, value)
		} else {
			tree.right.Insert(key, value)
		}
	}
}

func (tree *Tree) Search(key []byte) (bool, int64) {
	if bytes.Compare(tree.key, key) == 0 {
		return true, tree.value
	} else {
		if tree.left != nil && bytes.Compare(key, tree.key) < 0 {
			return tree.left.Search(key)
		} else if tree.right != nil && bytes.Compare(key, tree.key) > 0 {
			return tree.right.Search(key)
		}
	}
	return false, 0
}

func (tree *Tree) IsLeaf() bool {
	return tree.key == nil
}

func (tree *Tree) Walk(fn WalkerFunc) {
	if !tree.left.IsLeaf() {
		tree.left.Walk(fn)
	}

	fn(tree.key, tree.value)

	if !tree.right.IsLeaf() {
		tree.right.Walk(fn)
	}
}

func (tree *Tree) Keys() [][]byte {
	var keys [][]byte
	tree.Walk(func(key []byte, _ int64) {
		keys = append(keys, key)
	})
	return keys
}
