package option

// temporary definitions
var GeneralUser = "metadb"

type Global struct {
	NOP bool // Placeholder
}

type Init struct {
	Global
	Datadir string
}

type Server struct {
	Global
	Debug          bool
	Trace          bool
	Datadir        string
	NoKafkaCommit  bool
	SourceFilename string
	LogSource      string
	Listen         string
	AdminPort      string
	TLSCert        string
	TLSKey         string
	NoTLS          bool
	MetadbVersion  string
}

type Stop struct {
	Global
	Datadir string
}

type Upgrade struct {
	Global
	Datadir string
}
