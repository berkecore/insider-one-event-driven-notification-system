package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/berkecore/insider-one-event-driven-notification-system/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TemplateRepo implements repository.TemplateRepository on PostgreSQL.
type TemplateRepo struct {
	pool *pgxpool.Pool
}

func NewTemplateRepo(pool *pgxpool.Pool) *TemplateRepo {
	return &TemplateRepo{pool: pool}
}

func (r *TemplateRepo) Create(ctx context.Context, tpl *domain.Template) error {
	err := r.pool.QueryRow(ctx, `
		INSERT INTO templates (id, name, channel, body)
		VALUES ($1, $2, $3, $4)
		RETURNING created_at, updated_at
	`, tpl.ID, tpl.Name, string(tpl.Channel), tpl.Body).Scan(&tpl.CreatedAt, &tpl.UpdatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return domain.ErrTemplateNameExists
		}
		return fmt.Errorf("create template: %w", err)
	}
	tpl.Variables = domain.ExtractTemplateVariables(tpl.Body)
	return nil
}

func (r *TemplateRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Template, error) {
	var (
		tpl domain.Template
		ch  string
	)
	err := r.pool.QueryRow(ctx, `
		SELECT id, name, channel, body, created_at, updated_at
		FROM templates WHERE id = $1
	`, id).Scan(&tpl.ID, &tpl.Name, &ch, &tpl.Body, &tpl.CreatedAt, &tpl.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrTemplateNotFound
		}
		return nil, fmt.Errorf("get template: %w", err)
	}
	tpl.Channel = domain.Channel(ch)
	tpl.Variables = domain.ExtractTemplateVariables(tpl.Body)
	return &tpl, nil
}

func (r *TemplateRepo) List(ctx context.Context, channel *domain.Channel) ([]domain.Template, error) {
	var (
		rows pgx.Rows
		err  error
	)
	if channel != nil {
		rows, err = r.pool.Query(ctx, `
			SELECT id, name, channel, body, created_at, updated_at
			FROM templates WHERE channel = $1
			ORDER BY created_at DESC
		`, string(*channel))
	} else {
		rows, err = r.pool.Query(ctx, `
			SELECT id, name, channel, body, created_at, updated_at
			FROM templates
			ORDER BY created_at DESC
		`)
	}
	if err != nil {
		return nil, fmt.Errorf("list templates: %w", err)
	}
	defer rows.Close()

	templates := make([]domain.Template, 0)
	for rows.Next() {
		var (
			tpl domain.Template
			ch  string
		)
		if err := rows.Scan(&tpl.ID, &tpl.Name, &ch, &tpl.Body, &tpl.CreatedAt, &tpl.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan template: %w", err)
		}
		tpl.Channel = domain.Channel(ch)
		tpl.Variables = domain.ExtractTemplateVariables(tpl.Body)
		templates = append(templates, tpl)
	}
	return templates, rows.Err()
}

func (r *TemplateRepo) Update(ctx context.Context, tpl *domain.Template) error {
	err := r.pool.QueryRow(ctx, `
		UPDATE templates
		SET name = $2, channel = $3, body = $4, updated_at = NOW()
		WHERE id = $1
		RETURNING created_at, updated_at
	`, tpl.ID, tpl.Name, string(tpl.Channel), tpl.Body).Scan(&tpl.CreatedAt, &tpl.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ErrTemplateNotFound
		}
		if isUniqueViolation(err) {
			return domain.ErrTemplateNameExists
		}
		return fmt.Errorf("update template: %w", err)
	}
	tpl.Variables = domain.ExtractTemplateVariables(tpl.Body)
	return nil
}

func (r *TemplateRepo) Delete(ctx context.Context, id uuid.UUID) error {
	tag, err := r.pool.Exec(ctx, `DELETE FROM templates WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete template: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrTemplateNotFound
	}
	return nil
}
