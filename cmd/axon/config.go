package main

// Config store configuration for axon
type Config struct {
	// source db credentials
	SourceDBHost string `envconfig:"source_db_host"`
	SourceDBPort int    `envconfig:"source_db_port"`
	SourceDBName string `envconfig:"source_db_name"`
	SourceDBUser string `envconfig:"source_db_user"`
	SourceDBPass string `envconfig:"source_db_pass"`

	// target db credentials
	TargetDBHost   string `envconfig:"target_db_host"`
	TargetDBPort   int    `envconfig:"target_db_port"`
	TargetDBName   string `envconfig:"target_db_name"`
	TargetDBUser   string `envconfig:"target_db_user"`
	TargetDBPass   string `envconfig:"target_db_pass"`
	TargetDBSchema string `envconfig:"target_db_schema" default:"public"`
}
