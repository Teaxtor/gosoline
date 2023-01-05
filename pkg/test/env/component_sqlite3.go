package env

import (
	"testing"

	"github.com/justtrackio/gosoline/pkg/cfg"
)

type sqlite3Component struct {
	baseComponent
	sqlComponent
	credentials sqlite3Credentials
	inMemory    bool
}

func (c *sqlite3Component) SetT(t *testing.T) {
	c.sqlComponent.t = t
	c.baseComponent.SetT(t)
}

func (c *sqlite3Component) CfgOptions() []cfg.Option {
	advanced := map[string]interface{}{
		"_auth": "true",
		"_fk":   "true",
		"_loc":  "UTC",
	}

	if c.inMemory {
		advanced["mode"] = "memory"
		advanced["cache"] = "shared"
	}

	return []cfg.Option{
		cfg.WithConfigMap(map[string]interface{}{
			"db": map[string]interface{}{
				c.name: map[string]interface{}{
					"uri.host":           c.credentials.Host,
					"uri.user":           c.credentials.UserName,
					"uri.password":       c.credentials.UserPassword,
					"uri.database":       c.credentials.DatabaseName,
					"uri.port":           0,
					"migrations.enabled": true,
					"advanced":           advanced,
				},
			},
		}),
	}
}
