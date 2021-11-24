package upgrade

import (
	"fmt"
	"github.com/metadb-project/metadb/cmd/metadb/option"
)

func Upgrade(opt *option.Upgrade) error {
	// Require that a data directory be specified.
	if opt.Datadir == "" {
		return fmt.Errorf("data directory not specified")
	}
	return nil
}
