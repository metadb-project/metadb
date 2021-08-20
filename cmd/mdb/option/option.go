package option

type Global struct {
	Host          string
	AdminPort     string
	NoTLS         bool
	TLSSkipVerify bool
}

type Config struct {
	Attr   *string
	Val    *string
	Delete bool
	List   bool
	Global
}

type ConfigDatabase struct {
	Name            string
	Type            string
	DBHost          string
	DBPort          string
	DBName          string
	DBAdminUser     string
	DBAdminPassword string
	DBUsers         string
	DBSSLMode       string
	Global
}

type ConfigSource struct {
	Name             string
	Brokers          string
	Topics           []string
	Group            string
	SchemaPassFilter []string
	SchemaPrefix     string
	Databases        []string
	Global
}

type Status struct {
	Global
}

type Enable struct {
	Connectors []string
	Global
}

type Disable struct {
	Connectors []string
	Global
}
