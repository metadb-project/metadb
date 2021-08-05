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
	Name       string
	Type       string
	DBHost     string
	DBPort     string
	DBName     string
	DBUser     string
	DBPassword string
	DBSSLMode  string
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
