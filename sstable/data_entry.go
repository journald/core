package sstable

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"io"
)

type DataEntry struct {
	Key      []byte
	Checksum [md5.Size]byte
	DataLen  int64
	Data     []byte
	Offset   int64
}

func NewDataEntry(key, data []byte) DataEntry {
	sum := md5.Sum(data)

	return DataEntry{
		Key:      key,
		Checksum: sum,
		DataLen:  int64(len(data)),
		Data:     data,
	}
}

func (e DataEntry) Write(w io.Writer) error {
	err := binary.Write(w, binary.LittleEndian, int64(len(e.Key)))
	if err != nil {
		return err
	}

	_, err = w.Write(e.Key)
	if err != nil {
		return err
	}

	_, err = w.Write(e.Checksum[:])
	if err != nil {
		return err
	}

	err = binary.Write(w, binary.LittleEndian, e.DataLen)
	if err != nil {
		return err
	}

	_, err = w.Write(e.Data)
	if err != nil {
		return err
	}

	return nil
}

func ReadDataEntry(r io.ReadSeeker) (DataEntry, error) {
	var entry DataEntry

	// Get current offset
	offset, err := r.Seek(0, io.SeekCurrent)
	if err != nil {
		return entry, err
	}
	entry.Offset = offset

	// read key length
	var keyLen int64
	err = binary.Read(r, binary.LittleEndian, &keyLen)
	if err != nil {
		return entry, err
	}

	entry.Key = make([]byte, keyLen)
	_, err = r.Read(entry.Key)
	if err != nil {
		return entry, err
	}

	// read data checksum
	_, err = r.Read(entry.Checksum[:])
	if err != nil {
		return entry, err
	}

	// read data length
	err = binary.Read(r, binary.LittleEndian, &entry.DataLen)
	if err != nil {
		return entry, err
	}

	// read data
	entry.Data = make([]byte, entry.DataLen)
	_, err = r.Read(entry.Data)
	if err != nil {
		return entry, err
	}

	if entry.Checksum != md5.Sum(entry.Data) {
		return entry, CorruptedDataError(entry.Key)
	}

	return entry, nil
}

type CorruptedDataError []byte

func (key CorruptedDataError) Error() string {
	return fmt.Sprintf("Data for key '%s' checksum missmatch.", key)
}
