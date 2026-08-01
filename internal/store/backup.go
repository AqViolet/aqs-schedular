package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
)

// BackupStore creates a consistent point-in-time snapshot at path.
type BackupStore interface {
	Backup(context.Context, string) error
}

func (s *SQLite) Backup(ctx context.Context, path string) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return errors.New("backup destination is required")
	}
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("backup destination already exists: %s", path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `VACUUM INTO ?`, path); err != nil {
		_ = os.Remove(path)
		return fmt.Errorf("create SQLite backup: %w", err)
	}
	return nil
}

var _ BackupStore = (*SQLite)(nil)
