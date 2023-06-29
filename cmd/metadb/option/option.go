package option

// temporary definitions
//var GeneralUser = "metadb"

type Global struct {
}

type Init struct {
	Global
	Datadir     string
	DatabaseURI string
}

type Upgrade struct {
	Global
	Datadir string
	Force   bool
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
	Port           string
	TLSCert        string
	TLSKey         string
	NoTLS          bool
	RewriteJSON    bool
	MemoryLimit    float64
}

type Stop struct {
	Global
	Datadir string
}

type Sync struct {
	Global
	Datadir string
	Source  string
	Force   bool
}

type EndSync struct {
	Global
	Datadir string
	Source  string
	Force   bool
}
