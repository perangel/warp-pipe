package db

// ConnConfig stores database connection settigs.
type ConnConfig struct {
	DBHost string `envconfig:"DB_HOST"`
	DBPort uint16 `envconfig:"DB_PORT" default:"5432"`
	DBUser string `envconfig:"DB_USER"`
	DBPass string `envconfig:"DB_PASS"`
	DBName string `envconfig:"DB_NAME"`
}
