package store

import (
	"context"
	"fmt"
	"strings"
)

// Expected schema markers for durable readiness (migration 001_init).
const (
	ExpectedMigrationVersion = "001_init"
)

// RequiredTables are the domain tables the runtime must see (bootstrap also adds meta).
var RequiredTables = []string{
	"active_generation",
	"gameplay_metrics",
	"ingestion_conflicts",
	"processed_events",
	"projection_generations",
	"rating_statistics",
	"recovery_jobs",
	"recovery_leases",
	"recovery_page_checkpoints",
	"recovery_request_idempotency",
	"schema_migrations",
	"tournament_statistics",
}

// VerifySchema confirms ClickHouse connectivity and required analytics tables/version.
func VerifySchema(ctx context.Context, c *Client) error {
	if err := c.Ping(ctx); err != nil {
		return fmt.Errorf("clickhouse ping: %w", err)
	}
	dbIdent, err := QuoteIdent(c.Database())
	if err != nil {
		return err
	}
	for _, table := range RequiredTables {
		q := fmt.Sprintf(
			"SELECT count() FROM system.tables WHERE database = {db:String} AND name = {name:String}",
		)
		cell, err := c.QueryCell(ctx, q, map[string]string{"db": c.Database(), "name": table})
		if err != nil {
			return fmt.Errorf("schema table check %s: %w", table, err)
		}
		if cell != "1" {
			return fmt.Errorf("schema missing table %s.%s", c.Database(), table)
		}
		_ = dbIdent
	}
	ver, err := c.QueryCell(ctx, "SELECT version FROM schema_migrations FINAL LIMIT 1", nil)
	if err != nil {
		return fmt.Errorf("schema_migrations: %w", err)
	}
	if strings.TrimSpace(ver) != ExpectedMigrationVersion {
		return fmt.Errorf("schema version %q != %q", ver, ExpectedMigrationVersion)
	}
	return nil
}
