package marctab

import (
	"context"
	"fmt"

	"github.com/metadb-project/metadb/cmd/internal/marct"
	"github.com/metadb-project/metadb/cmd/metadb/catalog"
	"github.com/metadb-project/metadb/cmd/metadb/dbx"
	"github.com/metadb-project/metadb/cmd/metadb/log"
)

func RunMarctab(db dbx.DB, datadir string, cat *catalog.Catalog) error {
	dc, err := db.Connect()
	if err != nil {
		return fmt.Errorf("%v", err)
	}
	defer dbx.Close(dc)
	dcsuper, err := db.ConnectSuper()
	if err != nil {
		return fmt.Errorf("%v", err)
	}
	defer dbx.Close(dcsuper)

	users := []string{}
	opt := &marct.TransformOptions{
		FullUpdate:   false,
		Datadir:      datadir,
		Users:        users,
		TrigramIndex: false,
		NoIndexes:    false,
		Verbose:      0,
		CSVFileName:  "",
		SRSRecords:   "",
		SRSMarc:      "",
		SRSMarcAttr:  "",
		Metadb:       true,
		PrintErr: func(format string, v ...any) {
			log.Warning("marc__t: %s\n", fmt.Sprintf(format, v...))
		},
	}
	if err = marct.Run(opt); err != nil {
		return fmt.Errorf("%v", err)
	}
	if err = cat.TableUpdatedNow(dbx.Table{S: "folio_source_record", T: "marc__t"}); err != nil {
		return fmt.Errorf("writing table updated time: %v", err)
	}
	_ = cat.RemoveTableUpdated(dbx.Table{S: "folio_source_record", T: "marctab"})
	users, err = catalog.AllUsers(dc)
	if err != nil {
		return fmt.Errorf("%v", err)
	}
	for _, u := range users {
		_, _ = dc.Exec(context.TODO(), "GRANT SELECT ON folio_source_record.marc__t TO "+u)
	}
	log.Debug("marc__t: updated table folio_source_record.marc__t")
	return nil
}
