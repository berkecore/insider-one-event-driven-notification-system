package service

import (
	"context"
	"strings"

	"github.com/berkecore/insider-one-event-driven-notification-system/internal/domain"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/repository"
	"github.com/google/uuid"
)

// TemplateInput captures fields for creating or updating a template.
type TemplateInput struct {
	Name    string
	Channel domain.Channel
	Body    string
}

// TemplateService manages reusable notification templates.
type TemplateService struct {
	repo repository.TemplateRepository
}

func NewTemplateService(repo repository.TemplateRepository) *TemplateService {
	return &TemplateService{repo: repo}
}

func (s *TemplateService) Create(ctx context.Context, input TemplateInput) (*domain.Template, error) {
	ch := domain.Channel(strings.ToLower(strings.TrimSpace(string(input.Channel))))
	name := strings.TrimSpace(input.Name)
	if verr := domain.ValidateTemplate(name, ch, input.Body); verr != nil {
		return nil, verr
	}
	tpl := &domain.Template{
		ID:        uuid.New(),
		Name:      name,
		Channel:   ch,
		Body:      input.Body,
		Variables: domain.ExtractTemplateVariables(input.Body),
	}
	if err := s.repo.Create(ctx, tpl); err != nil {
		return nil, err
	}
	return tpl, nil
}

func (s *TemplateService) GetByID(ctx context.Context, id uuid.UUID) (*domain.Template, error) {
	return s.repo.GetByID(ctx, id)
}

func (s *TemplateService) List(ctx context.Context, channel *domain.Channel) ([]domain.Template, error) {
	return s.repo.List(ctx, channel)
}

func (s *TemplateService) Update(ctx context.Context, id uuid.UUID, input TemplateInput) (*domain.Template, error) {
	ch := domain.Channel(strings.ToLower(strings.TrimSpace(string(input.Channel))))
	name := strings.TrimSpace(input.Name)
	if verr := domain.ValidateTemplate(name, ch, input.Body); verr != nil {
		return nil, verr
	}
	tpl := &domain.Template{
		ID:        id,
		Name:      name,
		Channel:   ch,
		Body:      input.Body,
		Variables: domain.ExtractTemplateVariables(input.Body),
	}
	if err := s.repo.Update(ctx, tpl); err != nil {
		return nil, err
	}
	return tpl, nil
}

func (s *TemplateService) Delete(ctx context.Context, id uuid.UUID) error {
	return s.repo.Delete(ctx, id)
}

func (s *TemplateService) Preview(ctx context.Context, id uuid.UUID, vars map[string]string) (string, error) {
	tpl, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return "", err
	}
	return tpl.Render(vars)
}
