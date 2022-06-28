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
	Datadir       string
	Force         bool
	MetadbVersion string // Call util.MetadbVersion() instead
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
	MetadbVersion  string // Call util.MetadbVersion() instead
	RewriteJSON    bool
}

type Stop struct {
	Global
	Datadir string
}

type Reset struct {
	Global
	Datadir   string
	Origins   string
	Force     bool
	Connector string
}

type Clean struct {
	Global
	Datadir   string
	Origins   string
	Force     bool
	Connector string
}
