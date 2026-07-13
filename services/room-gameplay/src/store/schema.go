package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	ExpectedMigrationVersion = "001_init"
	ExpectedBootstrapVersion = "001_init"
	// ExpectedSchemaChecksum is SHA-256 of services/room-gameplay/migrations/001_init.sql bytes.
	ExpectedSchemaChecksum = "7f07c43f3836e148902d5cffdc2c830d0b0a5dd9909ee0f979153aa2d2b505b7"
)

// SchemaExpectation holds the exact version/checksum VerifySchema requires.
type SchemaExpectation struct {
	MigrationVersion string
	BootstrapVersion string
	Checksum         string
}

// DefaultSchemaExpectation returns embedded fingerprint constants, optionally
// overridden by ROOM_SCHEMA_CHECKSUM / ROOM_SCHEMA_VERSION env.
func DefaultSchemaExpectation() SchemaExpectation {
	exp := SchemaExpectation{
		MigrationVersion: ExpectedMigrationVersion,
		BootstrapVersion: ExpectedBootstrapVersion,
		Checksum:         ExpectedSchemaChecksum,
	}
	if v := strings.TrimSpace(os.Getenv("ROOM_SCHEMA_VERSION")); v != "" {
		exp.MigrationVersion = v
		exp.BootstrapVersion = v
	}
	if c := strings.TrimSpace(os.Getenv("ROOM_SCHEMA_CHECKSUM")); c != "" {
		exp.Checksum = strings.ToLower(c)
	}
	return exp
}

// ChecksumBytes returns the hex SHA-256 of migration SQL (unit-test helper).
func ChecksumBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// VerifySchema pings the writer and fail-closes on schema drift.
func VerifySchema(ctx context.Context, pool *pgxpool.Pool, exp SchemaExpectation) error {
	if pool == nil {
		return fmt.Errorf("nil pool")
	}
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping: %w", err)
	}
	if exp.MigrationVersion == "" {
		exp.MigrationVersion = ExpectedMigrationVersion
	}
	if exp.BootstrapVersion == "" {
		exp.BootstrapVersion = ExpectedBootstrapVersion
	}
	if exp.Checksum == "" {
		exp.Checksum = ExpectedSchemaChecksum
	}
	exp.Checksum = strings.ToLower(exp.Checksum)

	var migCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&migCount); err != nil {
		return fmt.Errorf("schema_migrations count: %w", err)
	}
	if migCount != 1 {
		return fmt.Errorf("schema drift: expected exactly 1 schema_migrations row, got %d", migCount)
	}
	var migVersion string
	if err := pool.QueryRow(ctx, `SELECT version FROM schema_migrations`).Scan(&migVersion); err != nil {
		return fmt.Errorf("schema_migrations version: %w", err)
	}
	if migVersion != exp.MigrationVersion {
		return fmt.Errorf("schema drift: migration version %q want %q", migVersion, exp.MigrationVersion)
	}

	var metaCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM schema_bootstrap_meta`).Scan(&metaCount); err != nil {
		return fmt.Errorf("schema_bootstrap_meta count: %w", err)
	}
	if metaCount != 1 {
		return fmt.Errorf("schema drift: expected exactly 1 schema_bootstrap_meta row, got %d", metaCount)
	}
	var metaVersion, metaChecksum string
	if err := pool.QueryRow(ctx, `SELECT version, checksum FROM schema_bootstrap_meta`).Scan(&metaVersion, &metaChecksum); err != nil {
		return fmt.Errorf("schema_bootstrap_meta read: %w", err)
	}
	if metaVersion != exp.BootstrapVersion {
		return fmt.Errorf("schema drift: bootstrap version %q want %q", metaVersion, exp.BootstrapVersion)
	}
	if strings.ToLower(metaChecksum) != exp.Checksum {
		return fmt.Errorf("schema drift: bootstrap checksum mismatch")
	}
	return nil
}

// ErrSchemaDrift is returned when VerifySchema detects an unexpected catalog state.
var ErrSchemaDrift = errors.New("schema drift")
