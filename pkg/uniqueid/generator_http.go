package uniqueid

import (
	"context"
	"fmt"

	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/encoding/json"
	"github.com/justtrackio/gosoline/pkg/http"
	"github.com/justtrackio/gosoline/pkg/log"
)

type GeneratorHttpSettings struct {
	ApiPort uint `cfg:"api_port" default:"8088"`
}

type generatorHttp struct {
	client http.Client
	url    string
}

func NewGeneratorHttp(config cfg.Config, logger log.Logger) (Generator, error) {
	settings := GeneratorHttpSettings{}
	config.UnmarshalKey("unique_id", &settings)

	client := http.NewHttpClient(config, logger)
	url := fmt.Sprintf("http://localhost:%d/nextId", settings.ApiPort)

	return &generatorHttp{
		client: client,
		url:    url,
	}, nil
}

func (g *generatorHttp) NextId(ctx context.Context) (*int64, error) {
	out := &NextIdResponse{}

	req := g.client.NewRequest().WithUrl(g.url)

	res, err := g.client.Get(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("could not make uniqueid request: %w", err)
	}

	err = json.Unmarshal(res.Body, out)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal uniqueid response: %w", err)
	}

	return &out.Id, nil
}
