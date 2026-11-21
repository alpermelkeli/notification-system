package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/alpermelkeli/notification-system/internal/domain"
)

// TemplateInput is the content of a template to create or replace (B-3, ADR-0010). Name and body
// are validated before the storage layer.
type TemplateInput struct {
	Name    string
	Channel domain.Channel
	Body    string
}

// TemplateRef records which template version produced a notification's content.
type TemplateRef struct {
	ID      uuid.UUID
	Version int
}

// TemplateFilter selects templates for ListTemplates. Zero values mean "no filter".
type TemplateFilter struct {
	Channel *domain.Channel
	// After is the cursor: the id of the last template of the previous page.
	After *uuid.UUID
	// Limit is the page size, clamped to [1, MaxListLimit]; 0 means DefaultListLimit.
	Limit int
}

// TemplatePage is one page of templates.
type TemplatePage struct {
	Items []domain.Template
	// Next is the cursor for the following page, or nil on the last page.
	Next *uuid.UUID
}

const templateColumns = `id, name, channel, body, version, created_at, updated_at`

func scanTemplate(row pgx.Row) (domain.Template, error) {
	var (
		t       domain.Template
		channel string
	)
	if err := row.Scan(&t.ID, &t.Name, &channel, &t.Body, &t.Version, &t.CreatedAt, &t.UpdatedAt); err != nil {
		return domain.Template{}, err
	}
	t.Channel, t.CreatedAt, t.UpdatedAt = domain.Channel(channel), t.CreatedAt.UTC(), t.UpdatedAt.UTC()
	return t, nil
}

// CreateTemplate stores a new template at version 1. A template with the same name returns
// domain.ErrAlreadyExists.
func (s *Store) CreateTemplate(ctx context.Context, in TemplateInput) (domain.Template, error) {
	t, err := scanTemplate(s.pool.QueryRow(ctx, `
		INSERT INTO templates (id, name, channel, body) VALUES ($1, $2, $3, $4)
		RETURNING `+templateColumns,
		domain.NewIDAt(s.now()), in.Name, string(in.Channel), in.Body))
	if err != nil {
		return domain.Template{}, templateError("create template", err)
	}
	return t, nil
}

// GetTemplate returns the template with the given id.
func (s *Store) GetTemplate(ctx context.Context, id uuid.UUID) (domain.Template, error) {
	t, err := scanTemplate(s.pool.QueryRow(ctx, `SELECT `+templateColumns+` FROM templates WHERE id = $1`, id))
	if err != nil {
		return domain.Template{}, templateError("get template", err)
	}
	return t, nil
}

// GetTemplates returns the templates with the given ids, by id, in one query. Unknown ids are
// left out of the map.
func (s *Store) GetTemplates(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]domain.Template, error) {
	out := make(map[uuid.UUID]domain.Template, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := s.pool.Query(ctx, `SELECT `+templateColumns+` FROM templates WHERE id = ANY($1)`, ids)
	if err != nil {
		return nil, fmt.Errorf("get templates: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		t, err := scanTemplate(rows)
		if err != nil {
			return nil, fmt.Errorf("get templates: %w", err)
		}
		out[t.ID] = t
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("get templates: %w", err)
	}
	return out, nil
}

// ListTemplates returns templates newest first with keyset pagination on id.
func (s *Store) ListTemplates(ctx context.Context, f TemplateFilter) (TemplatePage, error) {
	limit := clampLimit(f.Limit)
	var (
		conds []string
		args  []any
	)
	if f.Channel != nil {
		args = append(args, string(*f.Channel))
		conds = append(conds, fmt.Sprintf("channel = $%d", len(args)))
	}
	if f.After != nil {
		args = append(args, *f.After)
		conds = append(conds, fmt.Sprintf("id < $%d", len(args)))
	}
	where := ""
	if len(conds) > 0 {
		where = "WHERE " + strings.Join(conds, " AND ")
	}
	args = append(args, limit+1)
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`SELECT %s FROM templates %s ORDER BY id DESC LIMIT $%d`,
		templateColumns, where, len(args)), args...)
	if err != nil {
		return TemplatePage{}, fmt.Errorf("list templates: %w", err)
	}
	items, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (domain.Template, error) { return scanTemplate(r) })
	if err != nil {
		return TemplatePage{}, fmt.Errorf("list templates: %w", err)
	}
	page := TemplatePage{Items: items}
	if len(items) > limit {
		page.Items = items[:limit]
		next := page.Items[limit-1].ID
		page.Next = &next
	}
	return page, nil
}

// UpdateTemplate replaces a template's name and body and increments its version. Notifications
// already created from it keep their rendered content (ADR-0010). The channel cannot change,
// because the body was validated for it: a different channel returns domain.ErrInvalidArgument.
func (s *Store) UpdateTemplate(ctx context.Context, id uuid.UUID, in TemplateInput) (domain.Template, error) {
	t, err := scanTemplate(s.pool.QueryRow(ctx, `
		UPDATE templates SET name = $3, body = $4, version = version + 1, updated_at = now()
		 WHERE id = $1 AND channel = $2
		RETURNING `+templateColumns, id, string(in.Channel), in.Name, in.Body))
	if errors.Is(err, pgx.ErrNoRows) {
		current, gerr := s.GetTemplate(ctx, id)
		if gerr != nil {
			return domain.Template{}, gerr
		}
		return domain.Template{}, fmt.Errorf("%w: the channel of a template cannot change (it is %s)",
			domain.ErrInvalidArgument, current.Channel)
	}
	if err != nil {
		return domain.Template{}, templateError("update template", err)
	}
	return t, nil
}

// DeleteTemplate deletes a template. Notifications created from it are not affected.
func (s *Store) DeleteTemplate(ctx context.Context, id uuid.UUID) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM templates WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete template: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrNotFound
	}
	return nil
}

// templateError maps no rows to domain.ErrNotFound and a unique violation of the name to
// domain.ErrAlreadyExists.
func templateError(op string, err error) error {
	var pgErr *pgconn.PgError
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return domain.ErrNotFound
	case errors.As(err, &pgErr) && pgErr.Code == "23505":
		return fmt.Errorf("%w: a template with this name", domain.ErrAlreadyExists)
	}
	return fmt.Errorf("%s: %w", op, err)
}
