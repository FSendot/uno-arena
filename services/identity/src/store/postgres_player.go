package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"unoarena/services/identity/domain"
)

// PlayerStore implements domain.PlayerRepository against Postgres.
type PlayerStore struct {
	pool *pgxpool.Pool
}

func NewPlayerStore(pool *pgxpool.Pool) *PlayerStore {
	return &PlayerStore{pool: pool}
}

func (s *PlayerStore) Save(ctx context.Context, player domain.Player) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO players (player_id, username, password_hash, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $4)
		ON CONFLICT (player_id) DO UPDATE SET
			username = EXCLUDED.username,
			password_hash = EXCLUDED.password_hash,
			updated_at = EXCLUDED.updated_at
	`, player.ID.String(), player.Username, player.PasswordHash, player.CreatedAt.UTC())
	if err != nil {
		if isUniqueViolation(err) {
			return domain.ErrUsernameTaken
		}
		return domain.WrapUnavailable(err)
	}
	return nil
}

func (s *PlayerStore) FindByUsername(ctx context.Context, username string) (domain.Player, bool, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT player_id, username, password_hash, created_at
		FROM players WHERE username = $1
	`, username)
	p, err := scanPlayer(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Player{}, false, nil
	}
	if err != nil {
		return domain.Player{}, false, domain.WrapUnavailable(err)
	}
	return p, true, nil
}

func (s *PlayerStore) FindByID(ctx context.Context, id domain.PlayerID) (domain.Player, bool, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT player_id, username, password_hash, created_at
		FROM players WHERE player_id = $1
	`, id.String())
	p, err := scanPlayer(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Player{}, false, nil
	}
	if err != nil {
		return domain.Player{}, false, domain.WrapUnavailable(err)
	}
	return p, true, nil
}

func (s *PlayerStore) RegisterWithDefaultACL(ctx context.Context, player domain.Player, role string) error {
	if role == "" {
		role = "player"
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.WrapUnavailable(err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO players (player_id, username, password_hash, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $4)
	`, player.ID.String(), player.Username, player.PasswordHash, player.CreatedAt.UTC())
	if err != nil {
		if isUniqueViolation(err) {
			return domain.ErrUsernameTaken
		}
		return domain.WrapUnavailable(err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO player_acls (player_id, role) VALUES ($1, $2)
	`, player.ID.String(), role)
	if err != nil {
		return domain.WrapUnavailable(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.WrapUnavailable(err)
	}
	return nil
}

func (s *PlayerStore) ListRoles(ctx context.Context, playerID domain.PlayerID) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT role FROM player_acls WHERE player_id = $1 ORDER BY role
	`, playerID.String())
	if err != nil {
		return nil, domain.WrapUnavailable(err)
	}
	defer rows.Close()
	var roles []string
	for rows.Next() {
		var role string
		if err := rows.Scan(&role); err != nil {
			return nil, domain.WrapUnavailable(err)
		}
		roles = append(roles, role)
	}
	if err := rows.Err(); err != nil {
		return nil, domain.WrapUnavailable(err)
	}
	return roles, nil
}

func (s *PlayerStore) LinkOrResolveExternal(ctx context.Context, issuer, subject, preferredUsername string, newPlayer domain.Player, acceptedRoles []string) (domain.Player, []string, error) {
	roles := domain.NormalizeAcceptedRoles(acceptedRoles, acceptedRoles)
	const maxAttempts = 5
	for attempt := 0; attempt < maxAttempts; attempt++ {
		player, persisted, err := s.linkOrResolveOnce(ctx, issuer, subject, preferredUsername, newPlayer, roles)
		if err == nil {
			return player, persisted, nil
		}
		if errors.Is(err, domain.ErrExternalIdentityConflict) {
			// Winner committed; rollback already happened — requery/retry.
			continue
		}
		return domain.Player{}, nil, err
	}
	// Final requery after exhausted races.
	return s.linkOrResolveOnce(ctx, issuer, subject, preferredUsername, newPlayer, roles)
}

func (s *PlayerStore) linkOrResolveOnce(ctx context.Context, issuer, subject, preferredUsername string, newPlayer domain.Player, roles []string) (domain.Player, []string, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Player{}, nil, domain.WrapUnavailable(err)
	}
	defer tx.Rollback(ctx)

	var existingID string
	err = tx.QueryRow(ctx, `
		SELECT player_id FROM external_identities
		WHERE issuer = $1 AND subject = $2
		FOR UPDATE
	`, issuer, subject).Scan(&existingID)
	if err == nil {
		row := tx.QueryRow(ctx, `
			SELECT player_id, username, password_hash, created_at
			FROM players WHERE player_id = $1
		`, existingID)
		p, scanErr := scanPlayer(row)
		if scanErr != nil {
			return domain.Player{}, nil, domain.WrapUnavailable(scanErr)
		}
		if err := reconcileACLsTx(ctx, tx, p.ID, roles); err != nil {
			return domain.Player{}, nil, err
		}
		persisted, roleErr := listRolesTx(ctx, tx, p.ID)
		if roleErr != nil {
			return domain.Player{}, nil, roleErr
		}
		if err := tx.Commit(ctx); err != nil {
			return domain.Player{}, nil, domain.WrapUnavailable(err)
		}
		return p, persisted, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return domain.Player{}, nil, domain.WrapUnavailable(err)
	}

	username := preferredUsername
	if username == "" {
		username = newPlayer.Username
	}
	if username == "" {
		username = newPlayer.ID.String()
	}
	var taken bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM players WHERE username = $1)`, username).Scan(&taken); err != nil {
		return domain.Player{}, nil, domain.WrapUnavailable(err)
	}
	if taken {
		username = username + "-" + newPlayer.ID.String()
	}
	player := newPlayer
	player.Username = username

	_, err = tx.Exec(ctx, `
		INSERT INTO players (player_id, username, password_hash, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $4)
	`, player.ID.String(), player.Username, player.PasswordHash, player.CreatedAt.UTC())
	if err != nil {
		if isUniqueViolation(err) {
			return domain.Player{}, nil, domain.ErrUsernameTaken
		}
		return domain.Player{}, nil, domain.WrapUnavailable(err)
	}
	if err := reconcileACLsTx(ctx, tx, player.ID, roles); err != nil {
		return domain.Player{}, nil, err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO external_identities (issuer, subject, player_id, linked_at)
		VALUES ($1, $2, $3, $4)
	`, issuer, subject, player.ID.String(), time.Now().UTC())
	if err != nil {
		if isUniqueViolation(err) {
			// Transaction rolls back — no orphan player.
			return domain.Player{}, nil, domain.ErrExternalIdentityConflict
		}
		return domain.Player{}, nil, domain.WrapUnavailable(err)
	}
	persisted, roleErr := listRolesTx(ctx, tx, player.ID)
	if roleErr != nil {
		return domain.Player{}, nil, roleErr
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Player{}, nil, domain.WrapUnavailable(err)
	}
	return player, persisted, nil
}

func reconcileACLsTx(ctx context.Context, tx pgx.Tx, playerID domain.PlayerID, roles []string) error {
	if len(roles) == 0 {
		roles = []string{"player"}
	}
	if _, err := tx.Exec(ctx, `DELETE FROM player_acls WHERE player_id = $1`, playerID.String()); err != nil {
		return domain.WrapUnavailable(err)
	}
	for _, role := range roles {
		if _, err := tx.Exec(ctx, `
			INSERT INTO player_acls (player_id, role) VALUES ($1, $2)
			ON CONFLICT (player_id, role) DO NOTHING
		`, playerID.String(), role); err != nil {
			return domain.WrapUnavailable(err)
		}
	}
	return nil
}

type scannable interface {
	Scan(dest ...any) error
}

func scanPlayer(row scannable) (domain.Player, error) {
	var (
		id, username, hash string
		created            time.Time
	)
	if err := row.Scan(&id, &username, &hash, &created); err != nil {
		return domain.Player{}, err
	}
	return domain.Player{
		ID:           domain.PlayerID(id),
		Username:     username,
		PasswordHash: hash,
		CreatedAt:    created.UTC(),
	}, nil
}

func listRolesTx(ctx context.Context, tx pgx.Tx, playerID domain.PlayerID) ([]string, error) {
	rows, err := tx.Query(ctx, `SELECT role FROM player_acls WHERE player_id = $1 ORDER BY role`, playerID.String())
	if err != nil {
		return nil, domain.WrapUnavailable(err)
	}
	defer rows.Close()
	var roles []string
	for rows.Next() {
		var role string
		if err := rows.Scan(&role); err != nil {
			return nil, domain.WrapUnavailable(err)
		}
		roles = append(roles, role)
	}
	if err := rows.Err(); err != nil {
		return nil, domain.WrapUnavailable(err)
	}
	return roles, nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// Ensure PlayerStore implements the interface at compile time.
var _ domain.PlayerRepository = (*PlayerStore)(nil)

// ErrCapabilityOnly is returned for memory/capability-only session outbox methods.
var ErrCapabilityOnly = fmt.Errorf("%w: outbox polling is capability-mode only", domain.ErrNotAllowed)
