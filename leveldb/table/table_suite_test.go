package table

import (
	"testing"

	"github.com/manorama-pidapa/goleveldb/leveldb/testutil"
)

func TestTable(t *testing.T) {
	testutil.RunSuite(t, "Table Suite")
}
