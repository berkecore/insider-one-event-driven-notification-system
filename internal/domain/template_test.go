package domain

import "testing"

func TestTemplateRenderAndVariables(t *testing.T) {
	tpl := &Template{
		Name:    "flash-sale",
		Channel: ChannelSMS,
		Body:    "Hi {{name}}, enjoy {{ discount }}% off with code {{code}}! Remember {{name}}, expires soon.",
	}

	vars := ExtractTemplateVariables(tpl.Body)
	expectedVars := []string{"code", "discount", "name"}
	if len(vars) != len(expectedVars) {
		t.Fatalf("expected %v, got %v", expectedVars, vars)
	}
	for i := range expectedVars {
		if vars[i] != expectedVars[i] {
			t.Fatalf("var[%d]: expected %s, got %s", i, expectedVars[i], vars[i])
		}
	}

	rendered, err := tpl.Render(map[string]string{
		"name":     "Berke",
		"discount": "25",
		"code":     "INSIDER25",
	})
	if err != nil {
		t.Fatalf("unexpected render error: %v", err)
	}
	want := "Hi Berke, enjoy 25% off with code INSIDER25! Remember Berke, expires soon."
	if rendered != want {
		t.Fatalf("expected %q, got %q", want, rendered)
	}

	// Missing required variable should fail cleanly
	if _, err := tpl.Render(map[string]string{"name": "Berke"}); err == nil {
		t.Fatal("expected error for missing template variables")
	}
}
