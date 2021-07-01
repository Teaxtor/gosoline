package ddb

import (
	"context"
	"fmt"

	"github.com/applike/gosoline/pkg/clock"

	"github.com/applike/gosoline/pkg/mdl"

	"github.com/applike/gosoline/pkg/cfg"
	"github.com/applike/gosoline/pkg/db-repo"
	"github.com/applike/gosoline/pkg/mon"
)

type crudRepository struct {
	logger mon.Logger

	repository Repository
	clock      clock.Clock
}

func NewDdbNotifying(config cfg.Config, logger mon.Logger, settings *Settings) (db_repo.CrudRepository, error) {
	base, _ := NewRepositoryCrud(config, logger, settings)
	nr := db_repo.NewNotifyingRepository(logger, base)

	modelId := mdl.ModelId{}
	modelId.PadFromConfig(config)
	// TODO modelId e.g. adwordsGeotarget
	version := 0
	var transformer mdl.TransformerResolver

	sn, err := db_repo.NewSnsNotifier(config, logger, modelId, version, transformer)
	if err != nil {
		return nil, err
	}

	nr.AddNotifierAll(sn)

	return nr, nil
}

func NewRepositoryCrud(config cfg.Config, logger mon.Logger, settings *Settings) (db_repo.CrudRepository, error) {
	base, err := NewRepository(config, logger, settings)
	if err != nil {
		return nil, fmt.Errorf("could not create base repository: %w", err)
	}

	return &crudRepository{
		logger:     logger,
		repository: base,
		clock:      clock.Provider,
	}, nil
}

func (r *crudRepository) Create(ctx context.Context, value db_repo.ModelBased) error {
	now := r.clock.Now()
	value.SetUpdatedAt(&now)
	value.SetCreatedAt(&now)

	_, err := r.repository.PutItem(ctx, nil, value)
	if err != nil {
		return fmt.Errorf("could not create item: %w", err)
	}

	return nil
}

type RangeKeyAware interface {
	GetRange() interface{}
}

func (r *crudRepository) Read(ctx context.Context, id *uint, out db_repo.ModelBased) error {
	qb := r.repository.GetItemBuilder()
	qb = qb.WithHash(*id)

	if rka, ok := out.(RangeKeyAware); ok {
		qb = qb.WithRange(rka)
	}

	_, err := r.repository.GetItem(ctx, qb, out)
	if err != nil {
		return fmt.Errorf("could not read item: %w", err)
	}

	return nil
}

func (r *crudRepository) Update(ctx context.Context, value db_repo.ModelBased) error {
	now := r.clock.Now()
	value.SetUpdatedAt(&now)

	return r.Create(ctx, value)
}

func (r *crudRepository) Delete(ctx context.Context, value db_repo.ModelBased) error {
	_, err := r.repository.DeleteItem(ctx, nil, value)
	if err != nil {
		return fmt.Errorf("could not delete item: %w", err)
	}

	return nil
}
