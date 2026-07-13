// Loads the demo parquet (NYC taxi trips, dev/demo/) into warzone over the
// Postgres wire protocol: derive the table from the parquet schema, CREATE
// TABLE it, then stream every row through one COPY per file.
//
// Run from the repo root, with warzone already up (`make run`):
//
//	go run ./dev/scripts/ingestion
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/parquet-go/parquet-go"
)

const (
	dsn   = "postgres://warzone@127.0.0.1:5432/warzone?sslmode=disable"
	table = "demo.trips"
	files = "dev/demo/*/*/*.parquet"
)

// A parquet column and the warzone column it becomes.
type column struct {
	name string
	sql  string
	// Timestamps have no warzone type (CREATE TABLE rejects TIMESTAMP), so they
	// land as TEXT. Non-nil means "format the int64 through this before writing".
	unit time.Duration
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	paths, err := filepath.Glob(files)
	if err != nil || len(paths) == 0 {
		return fmt.Errorf("no demo parquet found at %s (run from the repo root): %w", files, err)
	}

	cols, err := schemaOf(paths[0])
	if err != nil {
		return err
	}

	conn, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect (is warzone running? `make run`): %w", err)
	}
	defer conn.Close(ctx)

	defs := make([]string, len(cols))
	for i, c := range cols {
		defs[i] = c.name + " " + c.sql
	}
	// Deliberately not IF NOT EXISTS: that would silently append a second copy of
	// every row. warzone has no DROP TABLE yet, so a reload means wiping the
	// warehouse.
	ddl := fmt.Sprintf("CREATE TABLE %s (%s)", table, strings.Join(defs, ", "))
	if _, err := conn.Exec(ctx, ddl).ReadAll(); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("%s is already loaded — to reload: `make clean-data`, restart warzone, rerun", table)
		}
		return fmt.Errorf("%s: %w", ddl, err)
	}
	log.Printf("registered %s (%d columns)", table, len(cols))

	var total int64
	for _, path := range paths {
		n, err := load(ctx, conn, path, cols)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		total += n
		log.Printf("%-40s %6d rows", path, n)
	}
	log.Printf("done: %d rows into %s", total, table)
	return nil
}

// One COPY per file: rows stream through a pipe, so nothing is held in memory.
func load(ctx context.Context, conn *pgconn.PgConn, path string, cols []column) (int64, error) {
	reader, writer := io.Pipe()
	go func() { writer.CloseWithError(stream(path, cols, writer)) }()

	// Text format (tab-delimited, \N for null) rather than CSV: warzone's COPY
	// splits fields on the delimiter with no quote handling, and taxi data has
	// commas in it. It has no tabs.
	res, err := conn.CopyFrom(ctx, reader, fmt.Sprintf("COPY %s FROM STDIN", table))
	if err != nil {
		return 0, err
	}
	return res.RowsAffected(), nil
}

func stream(path string, cols []column, w io.Writer) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return err
	}
	pf, err := parquet.OpenFile(file, stat.Size())
	if err != nil {
		return err
	}

	// The partition values are the same for every row in the file.
	var suffix strings.Builder
	for _, kv := range partitionsOf(path) {
		suffix.WriteByte('\t')
		suffix.WriteString(kv[1])
	}

	var line strings.Builder
	buf := make([]parquet.Row, 500)

	for _, group := range pf.RowGroups() {
		rows := group.Rows()
		for {
			n, err := rows.ReadRows(buf)
			for _, row := range buf[:n] {
				line.Reset()
				for i, value := range row {
					if i > 0 {
						line.WriteByte('\t')
					}
					line.WriteString(field(value, cols[value.Column()]))
				}
				line.WriteString(suffix.String())
				line.WriteByte('\n')
				if _, err := io.WriteString(w, line.String()); err != nil {
					rows.Close()
					return err
				}
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				rows.Close()
				return err
			}
		}
		rows.Close()
	}
	return nil
}

// Hive partition columns (`year=2021/month=1`) live in the directory names, not
// inside the parquet — without this the demo silently loses two columns.
func partitionsOf(path string) [][2]string {
	var out [][2]string
	for _, segment := range strings.Split(filepath.ToSlash(path), "/") {
		if key, value, ok := strings.Cut(segment, "="); ok {
			out = append(out, [2]string{key, value})
		}
	}
	return out
}

func field(v parquet.Value, c column) string {
	if v.IsNull() {
		return `\N`
	}
	if c.unit != 0 {
		return time.Unix(0, v.Int64()*int64(c.unit)).UTC().Format(time.RFC3339)
	}
	return v.String()
}

// Parquet schema -> warzone columns. Derived, not hardcoded, so this does not
// rot if the demo data changes.
func schemaOf(path string) ([]column, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	pf, err := parquet.OpenFile(file, stat.Size())
	if err != nil {
		return nil, err
	}

	fields := pf.Schema().Fields()
	cols := make([]column, len(fields))
	for i, f := range fields {
		c := column{name: f.Name()}

		if unit := timestampUnit(f.Type()); unit != 0 {
			// warzone has no timestamp type yet — CREATE TABLE rejects TIMESTAMP.
			// Keep the instant readable rather than dumping epoch integers.
			c.sql, c.unit = "TEXT", unit
			cols[i] = c
			continue
		}

		switch f.Type().Kind() {
		case parquet.Boolean:
			c.sql = "BOOLEAN"
		case parquet.Int32:
			c.sql = "INT"
		case parquet.Int64:
			c.sql = "BIGINT"
		case parquet.Float:
			c.sql = "REAL"
		case parquet.Double:
			c.sql = "DOUBLE PRECISION"
		default:
			c.sql = "TEXT"
		}
		cols[i] = c
	}

	for _, kv := range partitionsOf(path) {
		sql := "TEXT"
		if _, err := strconv.ParseInt(kv[1], 10, 64); err == nil {
			sql = "BIGINT"
		}
		cols = append(cols, column{name: kv[0], sql: sql})
	}
	return cols, nil
}

func timestampUnit(t parquet.Type) time.Duration {
	logical := t.LogicalType()
	if logical == nil || logical.Timestamp == nil {
		return 0
	}
	switch unit := logical.Timestamp.Unit; {
	case unit.Millis != nil:
		return time.Millisecond
	case unit.Micros != nil:
		return time.Microsecond
	case unit.Nanos != nil:
		return time.Nanosecond
	}
	return 0
}
