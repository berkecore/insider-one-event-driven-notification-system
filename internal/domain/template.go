package domain

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

var templateVarRegex = regexp.MustCompile(`\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}`)

// Template represents a reusable message template with {{variable}} placeholders.
type Template struct {
	ID        uuid.UUID `json:"id"`
	Name      string    `json:"name"`
	Channel   Channel   `json:"channel"`
	Body      string    `json:"body"`
	Variables []string  `json:"variables"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// ExtractTemplateVariables returns the sorted unique list of placeholder variable names in body.
func ExtractTemplateVariables(body string) []string {
	matches := templateVarRegex.FindAllStringSubmatch(body, -1)
	if len(matches) == 0 {
		return []string{}
	}
	seen := make(map[string]struct{}, len(matches))
	vars := make([]string, 0, len(matches))
	for _, m := range matches {
		name := m[1]
		if _, ok := seen[name]; !ok {
			seen[name] = struct{}{}
			vars = append(vars, name)
		}
	}
	sort.Strings(vars)
	return vars
}

// Render substitutes all {{variable}} placeholders using vars.
// It fails with a ValidationError if any required placeholder is missing.
func (t *Template) Render(vars map[string]string) (string, error) {
	required := ExtractTemplateVariables(t.Body)
	if len(required) == 0 {
		return t.Body, nil
	}

	var missing []string
	for _, key := range required {
		if _, ok := vars[key]; !ok {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		verr := &ValidationError{}
		verr.Add("template_vars", fmt.Sprintf("missing required template variables: %s", strings.Join(missing, ", ")))
		return "", verr
	}

	rendered := templateVarRegex.ReplaceAllStringFunc(t.Body, func(match string) string {
		sub := templateVarRegex.FindStringSubmatch(match)
		if len(sub) < 2 {
			return match
		}
		return vars[sub[1]]
	})
	return rendered, nil
}
