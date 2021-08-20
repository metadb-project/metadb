package sqlx

import (
	"testing"
)

func TestCSVToSQLBasic(t *testing.T) {
	var csv = "first,second,third"
	var wantSQL = "'first','second','third'"
	var gotSQL string
	gotSQL = CSVToSQL(csv)
	if gotSQL != wantSQL {
		t.Errorf("got %v; want %v", gotSQL, wantSQL)
	}
}

func TestCSVToSQLNone(t *testing.T) {
	var csv = ""
	var wantSQL = ""
	var gotSQL string
	gotSQL = CSVToSQL(csv)
	if gotSQL != wantSQL {
		t.Errorf("got %v; want %v", gotSQL, wantSQL)
	}
}

func TestCSVToSQLOne(t *testing.T) {
	var csv = "first"
	var wantSQL = "'first'"
	var gotSQL string
	gotSQL = CSVToSQL(csv)
	if gotSQL != wantSQL {
		t.Errorf("got %v; want %v", gotSQL, wantSQL)
	}
}

func TestCSVToSQLEmptyElement(t *testing.T) {
	var csv = "first,,third"
	var wantSQL = "'first','','third'"
	var gotSQL string
	gotSQL = CSVToSQL(csv)
	if gotSQL != wantSQL {
		t.Errorf("got %v; want %v", gotSQL, wantSQL)
	}
}
