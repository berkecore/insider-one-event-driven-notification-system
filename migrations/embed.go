package migrations

import "embed"

// FS embeds all versioned SQL migration files for automatic startup migration.
//
//go:embed *.sql
var FS embed.FS
