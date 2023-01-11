package catalog

//func FolioTenant(db *dbx.DB) (string, error) {
//	dc, err := db.Connect()
//	if err != nil {
//		return "", fmt.Errorf("connecting to database: %v", err)
//	}
//	defer dbx.Close(dc)
//
//	var tenant string
//	err = dc.QueryRow(context.TODO(), "SELECT val FROM metadb.folio WHERE attr='tenant'").Scan(&tenant)
//	switch {
//	case err == sql.ErrNoRows:
//		return "", fmt.Errorf("select tenant: %v", err)
//	case err != nil:
//		return "", err
//	default:
//		return tenant, nil
//	}
//}

//func ReshareTenants(db *dbx.DB) (string, error) {
//	dc, err := db.Connect()
//	if err != nil {
//		return "", fmt.Errorf("connecting to database: %v", err)
//	}
//	defer dbx.Close(dc)
//
//	var tenants string
//	err = dc.QueryRow(context.TODO(), "SELECT name FROM metadb.origin").Scan(&tenants)
//	switch {
//	case err == sql.ErrNoRows:
//		return "", fmt.Errorf("select tenants: %v", err)
//	case err != nil:
//		return "", err
//	default:
//		return tenants, nil
//	}
//}
