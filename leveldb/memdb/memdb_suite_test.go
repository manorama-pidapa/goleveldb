package memdb

import (
	"testing"

	"github.com/manorama-pidapa/goleveldb/leveldb/testutil"
)

func TestMemDB(t *testing.T) {
	testutil.RunSuite(t, "MemDB Suite")
}
