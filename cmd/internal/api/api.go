package api

import (
	"github.com/metadb-project/metadb/cmd/internal/status"
)

type ConfigListRequest struct {
	Attr string
}

type ConfigListResponse struct {
	Configs []ConfigItem
}

type ConfigItem struct {
	Attr string
	Val  string
}

type ConfigUpdateRequest struct {
	Attr string
	Val  string
}

type ConfigDeleteRequest struct {
	Attr string
}

type ConfigDeleteResponse struct {
	AttrNotFound bool
}

type UserListRequest struct {
	Name string
}

type UserListResponse struct {
	Users []UserItem
}

type UserItem struct {
	Name   string
	Tables string
}

type UserUpdateRequest struct {
	Name     string
	Tables   string
	Create   bool
	Password string
}

type UserDeleteRequest struct {
	Name string
}

type UserDeleteResponse struct {
	NameNotFound bool
}

type UpdateDatabaseConnectorRequest DatabaseConnector

type DatabaseConnector struct {
	Name   string                  `json:"name"`
	Config DatabaseConnectorConfig `json:"config"`
}

type DatabaseConnectorConfig struct {
	Type            string `json:"type"`
	DBHost          string `json:"dbHost"`
	DBPort          string `json:"dbPort"`
	DBName          string `json:"dbName"`
	DBAdminUser     string `json:"dbAdminUser"`
	DBAdminPassword string `json:"dbAdminPassword"`
	DBUsers         string `json:"dbUsers"`
	DBSSLMode       string `json:"dbSSLMode"`
}

type UpdateSourceConnectorRequest SourceConnector

type SourceConnector struct {
	Name   string                `json:"name"`
	Config SourceConnectorConfig `json:"config"`
}

type SourceConnectorConfig struct {
	Brokers          string   `json:"brokers"`
	Topics           []string `json:"topics"`
	Group            string   `json:"group"`
	SchemaPassFilter []string `json:"schemaPassFilter"`
	SchemaPrefix     string   `json:"schemaPrefix"`
	Databases        []string `json:"databases"`
}

type GetStatusRequest struct {
	// NOP
}

type GetStatusResponse struct {
	Sources   map[string]status.Status `json:"sources"`
	Databases map[string]status.Status `json:"databases"`
}

type EnableRequest struct {
	Connectors []string
}

type DisableRequest struct {
	Connectors []string
}
