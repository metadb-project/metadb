package option

import "github.com/metadb-project/metadb/cmd/metadb/command"

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
	Debug         bool
	Trace         bool
	Datadir       string
	NoKafkaCommit bool
	LogSource     string
	Listen        string
	Port          string
	TLSCert       string
	TLSKey        string
	NoTLS         bool
	MemoryLimit   float64
	UUOpt         bool
	Script        bool
	ScriptOpts    ScriptOptions
}

type ScriptOptions struct {
	CmdGraph *command.CommandGraph
}

type Stop struct {
	Global
	Datadir string
}

type Sync struct {
	Global
	Datadir  string
	Source   string
	Force    bool
	ForceAll bool
}

type Verify struct {
	Global
	Datadir string
}

type EndSync struct {
	Global
	Datadir  string
	Source   string
	Force    bool
	ForceAll bool
}

type Migrate struct {
	Global
	Datadir string
	Source  string
	LDPConf string
}
