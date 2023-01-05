package db

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"
	_ "github.com/mattn/go-sqlite3"
)

const (
	DriverSqlite    = "sqlite3"
	filePermissions = 0o700
)

func init() {
	connectionFactories[DriverSqlite] = NewSqliteDriverFactory()
	advancedSettingsUnmarshaller[DriverSqlite] = &sqliteSettings{}
}

func NewSqliteDriverFactory() DriverFactory {
	return &sqliteDriverFactory{}
}

type sqliteDriverFactory struct{}

type sqliteSettings struct {
	Auth               bool   `cfg:"_auth" default:"false"`
	ForeignKeysEnabled string `cfg:"_fk" default:"true"`
	TimeLocation       string `cfg:"_loc" default:"UTC"`
	Mode               string `cfg:"mode" default:"rwc"`
	Cache              string `cfg:"cache" default:"private"`
}

func (m *sqliteDriverFactory) GetDSN(settings Settings) (string, error) {
	// expand home dirs when required
	if strings.HasPrefix(settings.Uri.Host, "file:~") {
		if err := sanitizeFileSettings(&settings); err != nil {
			return "", err
		}
	}

	dsn := url.URL{
		Host: settings.Uri.Host,
		Path: settings.Uri.Database,
	}

	// TODO: go install -tags "sqlite_userauth sqlite_foreign_keys sqlite_json" github.com/mattn/go-sqlite3

	advanced := settings.Advanced.(*sqliteSettings)

	qry := dsn.Query()
	if advanced.Auth {
		// requires sqlite3 to be build with -tags sqlite_userauth
		qry.Set("_auth", "true")
		qry.Set("_auth_user", settings.Uri.User)
		qry.Set("_auth_pass", settings.Uri.Password)
	}

	qry.Set("_fk", advanced.ForeignKeysEnabled)
	qry.Set("_loc", advanced.TimeLocation)

	qry.Set("cache", advanced.Cache)
	qry.Set("mode", advanced.Mode)

	dsn.RawQuery = qry.Encode()

	uri := dsn.String()

	return uri[2:], nil
}

func (m *sqliteDriverFactory) GetMigrationDriver(db *sql.DB, database string, migrationsTable string) (database.Driver, error) {
	return sqlite3.WithInstance(db, &sqlite3.Config{
		DatabaseName:    database,
		MigrationsTable: migrationsTable,
	})
}

func sanitizeFileSettings(settings *Settings) error {
	dirName, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("unable to fetch home directory: %w", err)
	}

	dirPath := dirName + settings.Uri.Host[6:]

	// ensure db file exists
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		err = os.MkdirAll(dirPath, filePermissions)
		if err != nil {
			return fmt.Errorf("cannot create parent directories: %w", err)
		}
	}

	filePath := dirPath + string(os.PathSeparator) + settings.Uri.Database
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		_, err = os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, filePermissions)
		if err != nil {
			return fmt.Errorf("cannot create database file: %w", err)
		}
	}

	settings.Uri.Host = "file:" + dirPath

	return nil
}
