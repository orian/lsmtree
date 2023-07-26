// Package lsmstore implements a simple LSM tree in memory with a file storage for a data when changes exceeds a size.
package lsmstore

import (
	"encoding/gob"
	"os"
	"sort"

	"golang.org/x/exp/constraints"
	"golang.org/x/exp/slices"
)

type Value interface {
	Marshal() ([]byte, error)
	Unmarshal(data []byte) error
}

type Bytes []byte

func (b Bytes) Marshal() ([]byte, error) {
	// TODO implement me
	panic("implement me")
}

func (b Bytes) Unmarshal(data []byte) error {
	// TODO implement me
	panic("implement me")
}

type KeyType interface {
	constraints.Ordered
}

type Record[Key KeyType] struct {
	K Key
	V Value
}

type ValueStore[Key KeyType] []Record[Key]

func (v ValueStore[Key]) Len() int {
	return len(v)
}

func (v ValueStore[Key]) Less(i, j int) bool {
	return v[i].K < v[j].K
}

func (v ValueStore[Key]) Swap(i, j int) {
	v[i], v[j] = v[j], v[i]
}

type ByteSize uint64

// FileDescriptor describes data file on disk
type FileDescriptor[Key KeyType] struct {
	RecordsNum uint64   // number of records
	SizeBytes  ByteSize // size of the file in bytes

	KeySize   ByteSize // size of the key in bytes
	ValueSize ByteSize // size of the value in bytes

	RawKeySize   ByteSize
	RawValueSize ByteSize

	First Key
	Last  Key
}

func WriteData[Key KeyType](dataPath, metaPath string, data []Record[Key]) error {
	f, err := os.Create(dataPath)
	if err != nil {
		return err
	}
	gob.Register(Record[Key]{})
	enc := gob.NewEncoder(f)
	meta := FileDescriptor[Key]{
		RecordsNum:   uint64(len(data)),
		SizeBytes:    0,
		KeySize:      0,
		ValueSize:    0,
		RawKeySize:   0,
		RawValueSize: 0,
		// First:        nil,
		// Last:         nil,
	}
	if err := enc.Encode(meta); err != nil {
		return err
	}
	for _, v := range data {
		if err := enc.Encode(v.K); err != nil {
			return err
		}
		//if err := enc.Encode(v.V); err != nil {
		//	return err
		//}
	}
	return f.Close()
}

func ReadData[Key KeyType](filePath string) ([]Record[Key], error) {
	gob.Register(Record[Key]{})
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	dec := gob.NewDecoder(f)
	var numRecords uint64
	meta := FileDescriptor[Key]{}
	if err = dec.Decode(&meta); err != nil {
		return nil, err
	}
	numRecords = meta.RecordsNum
	ret := make([]Record[Key], 0, numRecords)
	// var rec Record[Key]
	var key Key
	for i := uint64(0); i < numRecords; i++ {
		if err = dec.Decode(&key); err != nil {
			return nil, err
		}
		ret = append(ret, Record[Key]{K: key})
	}
	return ret, nil
}

// LSMTree is a simple in-memory log structured merge tree.
type LSMTree[Key KeyType] struct {
	v []Record[Key]
}

const KMinFileSize = 100

func (l *LSMTree[Key]) Insert(k Key, v Value) error {
	l.v = append(l.v, Record[Key]{k, v})
	sort.Sort(ValueStore[Key](l.v))
	slices.SortFunc[Record[Key]](l.v, func(a, b Record[Key]) bool {
		return a.K < b.K
	})

	if len(l.v) > KMinFileSize {
		if err := l.SaveData(); err != nil {
			return err
		}
	}

	return nil
}

type Iterator struct{}

func (l *LSMTree[Key]) Find(k Key) (kv Record[Key], found bool) {
	idx := sort.Search(len(l.v), func(i int) bool {
		return l.v[i].K >= k
	})
	if idx < len(l.v) {
		return l.v[idx], true
	}
	return
}

func (l *LSMTree[Key]) SaveData() error {
	return nil
}

type FullLSMTree[Key KeyType] struct {
	Changes LSMTree[Key]
	Data    []FileDescriptor[Key] // all files in this LSM tree.
}

func (l *FullLSMTree[Key]) Insert(k Key, v Value) error {
	return l.Changes.Insert(k, v)
}

func (l *FullLSMTree[Key]) Find(k Key) (kv Record[Key], found bool) {
	return l.Changes.Find(k)
}
