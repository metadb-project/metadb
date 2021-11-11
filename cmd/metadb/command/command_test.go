package command

import (
	"testing"
)

func TestExtractOriginMatch(t *testing.T) {
	var prefixes = []string{"reshare_east", "reshare_north", "reshare_outer", "reshare_south", "reshare_west"}
	var schema = "reshare_west_inventory"
	var wantOrigin = "reshare_west"
	var wantNewSchema = "inventory"
	var gotOrigin, gotNewSchema string
	gotOrigin, gotNewSchema = extractOrigin(prefixes, schema)
	if gotOrigin != wantOrigin || gotNewSchema != wantNewSchema {
		t.Errorf("got %v, %v; want %v, %v", gotOrigin, gotNewSchema, wantOrigin, wantNewSchema)
	}
}

func TestExtractOriginNoMatch(t *testing.T) {
	var prefixes = []string{"reshare_east", "reshare_north", "reshare_outer", "reshare_south", "reshare_west"}
	var schema = "si_cardinal_circulation"
	var wantOrigin = ""
	var wantNewSchema = "si_cardinal_circulation"
	var gotOrigin, gotNewSchema string
	gotOrigin, gotNewSchema = extractOrigin(prefixes, schema)
	if gotOrigin != wantOrigin || gotNewSchema != wantNewSchema {
		t.Errorf("got %v, %v; want %v, %v", gotOrigin, gotNewSchema, wantOrigin, wantNewSchema)
	}
}

func TestExtractOriginEmptyPrefixes(t *testing.T) {
	var prefixes []string
	var schema = "reshare_west_inventory"
	var wantOrigin = ""
	var wantNewSchema = "reshare_west_inventory"
	var gotOrigin, gotNewSchema string
	gotOrigin, gotNewSchema = extractOrigin(prefixes, schema)
	if gotOrigin != wantOrigin || gotNewSchema != wantNewSchema {
		t.Errorf("got %v, %v; want %v, %v", gotOrigin, gotNewSchema, wantOrigin, wantNewSchema)
	}
}

func TestExtractOriginEmptySchema(t *testing.T) {
	var prefixes = []string{"reshare_east", "reshare_north", "reshare_outer", "reshare_south", "reshare_west"}
	var schema = ""
	var wantOrigin = ""
	var wantNewSchema = ""
	var gotOrigin, gotNewSchema string
	gotOrigin, gotNewSchema = extractOrigin(prefixes, schema)
	if gotOrigin != wantOrigin || gotNewSchema != wantNewSchema {
		t.Errorf("got %v, %v; want %v, %v", gotOrigin, gotNewSchema, wantOrigin, wantNewSchema)
	}
}

func TestExtractOriginEmpty(t *testing.T) {
	var prefixes []string
	var schema = ""
	var wantOrigin = ""
	var wantNewSchema = ""
	var gotOrigin, gotNewSchema string
	gotOrigin, gotNewSchema = extractOrigin(prefixes, schema)
	if gotOrigin != wantOrigin || gotNewSchema != wantNewSchema {
		t.Errorf("got %v, %v; want %v, %v", gotOrigin, gotNewSchema, wantOrigin, wantNewSchema)
	}
}
