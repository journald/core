package sstable

import (
	"bytes"
	"testing"
)

func TestDataWrite(t *testing.T) {
	buff := bytes.NewBufferString("")

	entry := NewDataEntry([]byte("foo"), []byte("bar"))
	entry.Write(buff)

	expected := []byte{0x3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x66, 0x6f, 0x6f, 0x37, 0xb5, 0x1d, 0x19, 0x4a, 0x75, 0x13, 0xe4, 0x5b, 0x56, 0xf6, 0x52, 0x4f, 0x2d, 0x51, 0xf2, 0x3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x62, 0x61, 0x72}

	if bytes.Compare(expected, buff.Bytes()) != 0 {
		t.Errorf("\nExpected: %#v\nGot:      %#v", expected, buff.Bytes())
	}
}

func TestDataRead(t *testing.T) {
	buff := bytes.NewBufferString("")

	entry := NewDataEntry([]byte("foo"), []byte("bar"))
	entry.Write(buff)

	reader := bytes.NewReader(buff.Bytes())

	read, err := ReadDataEntry(reader)
	if err != nil {
		t.Error(err)
	}

	if bytes.Compare(read.Key, []byte("foo")) != 0 {
		t.Errorf("Read a different key from what was previously written.\nExpected: %s\nGot:      %s", "foo", read.Key)
	}

	sum := [16]uint8{0x37, 0xb5, 0x1d, 0x19, 0x4a, 0x75, 0x13, 0xe4, 0x5b, 0x56, 0xf6, 0x52, 0x4f, 0x2d, 0x51, 0xf2}
	if bytes.Compare(read.Checksum[:], sum[:]) != 0 {
		t.Errorf("Read a different checksum from what was previously written.\nExpected: %v\nGot:      %v", sum, read.Checksum)
	}

	if read.DataLen != 3 {
		t.Errorf("Read a different data length from what was previously written.\nExpected: %v\nGot:      %v", 3, read.DataLen)
	}

	if bytes.Compare(read.Data, []byte("bar")) != 0 {
		t.Errorf("Read a different key from what was previously written.\nExpected: %s\nGot:      %s", "bar", read.Data)
	}
}
