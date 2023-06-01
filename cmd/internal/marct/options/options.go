package options

type Options struct {
	TempPartitionSchema  string
	TempTablePrefix      string
	PartitionTableBase   string
	FinalPartitionSchema string
}

func (o Options) SFPartitionTable(field, sf string) string {
	var sf0, sf1 string
	// If sf is uppercase, convert to lowercase and double it, so that we can send
	// the database a lowercase table name.
	if len(sf) != 0 && 'A' <= sf[0] && sf[0] <= 'Z' {
		sf0 = string(sf[0] ^ 0x20) // Convert to lowercase.
		sf1 = sf0
	} else {
		sf0 = sf
	}
	return o.PartitionTableBase + field + "_" + sf0 + sf1
}
