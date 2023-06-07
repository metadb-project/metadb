package options

import "strconv"

type Options struct {
	TempPartitionSchema  string
	TempTablePrefix      string
	PartitionTableBase   string
	FinalPartitionSchema string
}

func (o Options) SFPartitionTable(field, sf string) string {
	return o.PartitionTableBase + field + "_" + sfToIdentifierString(sf)
}

// sfToIdentifierString converts a valid sf to a string that is valid to use as the middle or end of a database
// identifier.
func sfToIdentifierString(sf string) string {
	if sf == "" {
		return ""
	}
	r := sf[0]
	if ('a' <= r && r <= 'z') || ('0' <= r && r <= '9') {
		return sf
	}
	if 'A' <= r && r <= 'Z' {
		w := string(r ^ 0x20) // Convert to lowercase.
		return w + w
	}
	return "0x" + strconv.FormatInt(int64(r), 16)
}
