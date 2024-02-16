package marctab

import (
	"context"
	"fmt"
	"time"

	"github.com/nazgaret/metadb/cmd/internal/libmarct"
	"github.com/nazgaret/metadb/cmd/metadb/catalog"
	"github.com/nazgaret/metadb/cmd/metadb/dbx"
	"github.com/nazgaret/metadb/cmd/metadb/log"
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

	start := time.Now()
	users := make([]string, 0)
	t := &marct.MARCTransform{
		FullUpdate: false,
		Datadir:    datadir,
		Users:      users,
		//TrigramIndex: false,
		//NoIndexes:    false,
		Verbose: 0,
		//CSVFileName:  "",
		SRSRecords:  "",
		SRSMarc:     "",
		SRSMarcAttr: "",
		Metadb:      true,
		PrintErr: func(format string, v ...any) {
			log.Warning("marc__t: %s\n", fmt.Sprintf(format, v...))
		},
	}
	if err = t.Transform(); err != nil {
		return fmt.Errorf("%v", err)
	}
	elapsed := time.Since(start)
	if err = cat.TableUpdatedNow(dbx.Table{Schema: "folio_source_record", Table: "marc__t"}, elapsed); err != nil {
		return fmt.Errorf("writing table updated time: %v", err)
	}
	_ = cat.RemoveTableUpdated(dbx.Table{Schema: "folio_source_record", Table: "marctab"})
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
