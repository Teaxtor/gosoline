package env

import (
	"fmt"
	"net/url"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
	"github.com/justtrackio/gosoline/pkg/uuid"
)

func init() {
	componentFactories[componentSqlite3] = new(sqlite3Factory)
}

const componentSqlite3 = "sqlite3"

type sqlite3Credentials struct {
	Host         string `cfg:"host" default:""` // keep default empty to allow users to specify a file on their own
	DatabaseName string `cfg:"database_name" default:"gosoline"`
	UserName     string `cfg:"user_name" default:"gosoline"`
	UserPassword string `cfg:"user_password" default:"gosoline"`
}

type sqlite3Settings struct {
	ComponentBaseSettings
	Credentials sqlite3Credentials `cfg:"credentials"`
	InMemory    bool               `cfg:"in_memory" default:"true"`
}

type sqlite3Factory struct{}

func (f sqlite3Factory) Detect(config cfg.Config, manager *ComponentsConfigManager) error {
	if !config.IsSet("db") {
		return nil
	}

	if manager.HasType(componentSqlite3) {
		return nil
	}

	components := config.GetStringMap("db")

	for name := range components {
		driver := config.Get(fmt.Sprintf("db.%s.driver", name))

		if driver != componentSqlite3 {
			continue
		}

		settings := &sqlite3Settings{}
		config.UnmarshalDefaults(settings)

		settings.Name = name
		settings.Type = componentSqlite3

		if err := manager.Add(settings); err != nil {
			return fmt.Errorf("can not add default sqlite3 component: %w", err)
		}
	}

	return nil
}

func (f sqlite3Factory) GetSettingsSchema() ComponentBaseSettingsAware {
	return &sqlite3Settings{}
}

func (f sqlite3Factory) DescribeContainers(settings interface{}) componentContainerDescriptions {
	return nil
}

func (f sqlite3Factory) Component(_ cfg.Config, _ log.Logger, _ map[string]*container, settings interface{}) (Component, error) {
	s := settings.(*sqlite3Settings)
	if s.Credentials.Host == "" {
		s.Credentials.Host = "file:" + uuid.New().NewV4()
	}

	client, err := f.connection(s)
	if err != nil {
		return nil, fmt.Errorf("can not create client: %w", err)
	}

	component := &sqlite3Component{
		baseComponent: baseComponent{
			name: s.Name,
		},
		sqlComponent: sqlComponent{
			client: client,
		},
		credentials: s.Credentials,
		inMemory:    s.InMemory,
	}

	return component, nil
}

func (f sqlite3Factory) connection(settings *sqlite3Settings) (*sqlx.DB, error) {
	dsn := url.URL{
		Host: settings.Credentials.Host,
		Path: settings.Credentials.DatabaseName,
	}

	qry := dsn.Query()
	qry.Set("_auth", "true")
	qry.Set("_auth_user", settings.Credentials.UserName)
	qry.Set("_auth_pass", settings.Credentials.UserPassword)

	qry.Set("_fk", "true")
	qry.Set("_loc", "UTC")

	if settings.InMemory {
		qry.Set("mode", "memory")
		qry.Set("cache", "shared")
	}

	dsn.RawQuery = qry.Encode()

	client, err := sqlx.Open("sqlite3", dsn.String()[2:])
	if err != nil {
		return nil, fmt.Errorf("can not create client: %w", err)
	}

	return client, nil
}
