package dbx

//func TestNewDB(t *testing.T) {
//	var uri = "postgres://metadb:mdbpass@a.b.c:5432/testdb?sslmode=require"
//	var want = DB{
//		Host:     "a.b.c",
//		Port:     "5432",
//		User:     "metadb",
//		Password: "mdbpass",
//		DBName:   "testdb",
//		SSLMode:  "require",
//	}
//	got, err := NewDB(uri)
//	if *got != want || err != nil {
//		t.Errorf("got %#v, %v; want %#v, <nil>", got, err, want)
//	}
//}

//func TestNewDBNoSSLMode(t *testing.T) {
//	var uri = "postgres://metadb:mdbpass@a.b.c:5432/testdb"
//	var want = DB{
//		Host:     "a.b.c",
//		Port:     "5432",
//		User:     "metadb",
//		Password: "mdbpass",
//		DBName:   "testdb",
//		SSLMode:  "",
//	}
//	got, err := NewDB(uri)
//	if *got != want || err != nil {
//		t.Errorf("got %#v, %v; want %#v, <nil>", got, err, want)
//	}
//}

//func TestNewDBNoUserinfo(t *testing.T) {
//	uri := "postgres://a.b.c:5432/testdb?sslmode=require"
//	_, err := NewDB(uri)
//	if err == nil {
//		t.Errorf("got <nil>")
//	}
//}
