# Design

## Ubiquitous Language

### Log Item

Element of the log. Contains a key []byte uniquely identifying the item, a type (a random
string) used for filtering purpose, some arbitrary data (binary) and a pointer
to a parent log item.

```
---------------
|    Item     |
---------------
| key: 42     |
| type: foo   |
| parent: 41  |
| data: ...   |
---------------
```

## Data Storage Design

Data querying patterns:

1. Lookup a single key
2. Read keys in order (insert order)
3. Check presence of a key in the log

In order to have efficient single key lookups, use _LSM based storage_. LSM Trees
maintain an in memory sorted key index which make querying faster.

In order to have efficient _"insert-ordered"_ reads, we modify the merge /
compaction step in LSM algorithm.

Merge algorithm is just concatenation of older data file with the newer one and
rewriting the key index with the new offsets.

This way we preserve the insert-order in data files and keeping key lookup fast.

Insert-order of data files allows to read log items sequentially easily once we
have found the start key without requiring key index lookups.

Using bloom filters make checking for absence of an item in the log fast (we are
not enforced to load all key index files

### SSTable

**Data file format**

```
key-size - key - data-size - data | key-size - key - data-size - data | ...
```

- Append only file
- Sorted by insert order

## Tools

### Server

`journald` binary to start a journald server

## Links

- [Log Structured Merge Trees](http://www.benstopford.com/2015/02/14/log-structured-merge-trees/)
- [LSM-Tree papers](http://www.cs.umb.edu/~poneil/lsmtree.pdf)
