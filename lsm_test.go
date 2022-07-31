package lsmstore

import (
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteReadData(t *testing.T) {
	want := []Record[int]{
		{
			K: 11,
			V: nil,
		},
		{
			K: 23,
			V: nil,
		},
	}
	d := t.TempDir()
	dataPath, metaPath := path.Join(d, "write.data"), path.Join(d, "write.meta")
	t.Logf("writing data into: %s", dataPath)
	require.NoError(t, WriteData(dataPath, metaPath, want))
	got, err := ReadData[int](dataPath)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestLSMTree_Find(t *testing.T) {
	tree := &LSMTree[int]{}
	for i := 0; i < 100; i++ {
		require.NoError(t, tree.Insert(i, Bytes{}))
	}
	tests := []struct {
		name      string
		argK      int
		wantKv    Record[int]
		wantFound bool
	}{
		{
			"",
			10,
			Record[int]{10, Bytes{}},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, gotFound := tree.Find(tt.argK)
			require.Equal(t, tt.wantFound, gotFound)
		})
	}
}
