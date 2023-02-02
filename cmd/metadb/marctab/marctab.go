package marctab

import (
	"context"
	"fmt"

	"github.com/library-data-platform/ldpmarc/marc"
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
	opt := &marc.TransformOptions{
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
		Vacuum:       false,
		PrintErr: func(format string, v ...any) {
			log.Warning("marctab: %s\n", fmt.Sprintf(format, v...))
		},
	}
	if err = marc.Run(opt); err != nil {
		return fmt.Errorf("%v", err)
	}
	if err = cat.TableUpdatedNow(dbx.Table{S: "folio_source_record", T: "marctab"}, true); err != nil {
		return fmt.Errorf("writing table updated time: %v", err)
	}
	users, err = catalog.AllUsers(dc)
	if err != nil {
		return fmt.Errorf("%v", err)
	}
	for _, u := range users {
		_, _ = dc.Exec(context.TODO(), "GRANT SELECT ON folio_source_record.marctab TO "+u)
	}
	for _, t := range []dbx.Table{{S: "marctab", T: "cksum"}, {S: "folio_source_record", T: "marctab"}} {
		log.Trace("vacuuming table %s", t)
		if err := dbx.Vacuum(dcsuper, t); err != nil {
			return err
		}
	}
	log.Debug("marctab: updated table folio_source_record.marctab")
	return nil
}
