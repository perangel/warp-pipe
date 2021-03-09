package warppipe

// AxonConfig store configuration for axon
type AxonConfig struct {
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

	// force Axon to shutdown after processing the latest changeset
	ShutdownAfterLastChangeset bool `envconfig:"shutdown_after_last_changeset"`

	// start the axon run from the specified changeset offset. defaults to 0.
	// Do not use specific changeset IDs, because they may not be consistent
	// between source and target.
	StartFromOffset int64 `envconfig:"start_from_offset" default:"0"`

	// Fail instead of skip when a duplicate row is found during insert.
	// Duplicates should never happen in some cases such as database migrations.
	FailOnDuplicate bool `envconfig:"fail_on_duplicate" default:"false"`
}
