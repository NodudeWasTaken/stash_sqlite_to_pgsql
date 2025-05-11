package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/postgres"
	_ "github.com/doug-martin/goqu/v9/dialect/sqlite3"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

var anon_dialect = goqu.Dialect("sqlite3")
var dialect = goqu.Dialect("postgres")

const restart_seq = `
SELECT setval(pg_get_serial_sequence('%[1]s', 'id')
            , COALESCE(max(id) + 1, 1)
            , false)
FROM %[1]s;
`

func open_sqlite(path string) (conn *sqlx.DB, err error) {
	const disableForeignKeys = false
	const writable = false

	// https://github.com/mattn/go-sqlite3
	url := "file:" + path + "?_journal=WAL&_sync=NORMAL&_busy_timeout=50"
	if !disableForeignKeys {
		url += "&_fk=true"
	}

	if writable {
		url += "&_txlock=immediate"
	} else {
		url += "&mode=ro"
	}

	conn, err = sqlx.Open("sqlite3", url)

	if err != nil {
		return nil, fmt.Errorf("db.Open(): %w", err)
	}

	return conn, nil
}

func open_pgsql(connector string) (conn *sqlx.DB, err error) {
	const disableForeignKeys = true
	const writable = true

	conn, err = sqlx.Open("pgx", connector)

	if err != nil {
		return nil, fmt.Errorf("db.Open(): %w", err)
	}

	if disableForeignKeys {
		_, err = conn.Exec("SET session_replication_role = replica;")

		if err != nil {
			return nil, fmt.Errorf("conn.Exec(): %w", err)
		}
	}
	if !writable {
		_, err = conn.Exec("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;")

		if err != nil {
			return nil, fmt.Errorf("conn.Exec(): %w", err)
		}
	}

	return conn, nil
}

func clampInt64ToInt32(value int64) int32 {
	if value > int64(math.MaxInt32) {
		return math.MaxInt32
	} else if value < int64(math.MinInt32) {
		return math.MinInt32
	}
	return int32(value)
}

// Helper: is the timestamp valid for Postgres?
func isValidPostgresTime(t time.Time) bool {
	// Postgres can't handle times before year 0001
	return t.Year() >= 1
}

func migrate(connector string, dbpath string) error {
	const batchSize = 1000

	sourceDB, err := open_sqlite(dbpath)
	if err != nil {
		return fmt.Errorf("failed to open db: %w", err)
	}

	destDB, err := open_pgsql(connector)
	if err != nil {
		return fmt.Errorf("failed to open db: %w", err)
	}

	// TODO: Check schema_migrations table for version
	// TODO: Call rollback if error

	ctx := context.Background()

	stxn, err := sourceDB.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("source begin tx: %w", err)
	}

	dtxn, err := destDB.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("dest begin tx: %w", err)
	}

	for _, table := range []string{
		"blobs",
		"files",
		"files_fingerprints",
		"folders",
		"galleries",
		"galleries_chapters",
		"galleries_files",
		"galleries_images",
		"galleries_tags",
		"gallery_urls",
		"group_urls",
		"groups",
		"groups_relations",
		"groups_scenes",
		"groups_tags",
		"image_files",
		"image_urls",
		"images",
		"images_files",
		"images_tags",
		"performer_aliases",
		"performer_stash_ids",
		"performer_urls",
		"performers",
		"performers_galleries",
		"performers_images",
		"performers_scenes",
		"performers_tags",
		"saved_filters",
		"scene_markers",
		"scene_markers_tags",
		"scene_stash_ids",
		"scene_urls",
		"scenes",
		"scenes_files",
		"scenes_galleries",
		"scenes_o_dates",
		"scenes_tags",
		"scenes_view_dates",
		"studio_aliases",
		"studio_stash_ids",
		"studios",
		"studios_tags",
		"tag_aliases",
		"tags",
		"tags_relations",
		"video_captions",
		"video_files",
	} {
		offset := 0

		fmt.Printf("Fetching %s\n", table)
		for {
			var rowsSlice []map[string]interface{}

			// Fetch
			{
				goquTable := goqu.I(table)
				q := anon_dialect.From(goquTable).Select(goquTable.All()).Limit(uint(batchSize)).Offset(uint(offset))
				sql, args, err := q.ToSQL()
				if err != nil {
					return fmt.Errorf("source failed tosql: %w", err)
				}

				r, err := stxn.QueryxContext(ctx, sql, args...)
				if err != nil {
					return fmt.Errorf("query `%s` [%v]: %w", sql, args, err)
				}

				for r.Next() {
					row := make(map[string]interface{})
					if err := r.MapScan(row); err != nil {
						return fmt.Errorf("failed structscan: %w", err)
					}
					rowsSlice = append(rowsSlice, row)
				}

				if len(rowsSlice) == 0 {
					break
				}
			}

			// Insert
			{
				// Hotfix the funspeed generator
				if table == "video_files" {
					for idx := range rowsSlice {
						if v, ok := rowsSlice[idx]["interactive_speed"].(int64); ok {
							rowsSlice[idx]["interactive_speed"] = clampInt64ToInt32(v)
						}
					}
				}
				if table == "performer_custom_fields" {
					for idx := range rowsSlice {
						rowsSlice[idx]["type"] = reflect.TypeOf(rowsSlice[idx]["value"]).String()
					}
				}
				if table == "saved_filters" {
					validRows := make([]map[string]interface{}, 0, len(rowsSlice))

					for _, row := range rowsSlice {
						valid := true
						for _, obj := range []string{"find_filter", "object_filter", "ui_options"} {
							if strVal, ok := row[obj].(string); ok {
								var tmp interface{}
								err := json.Unmarshal([]byte(strVal), &tmp)
								if err != nil {
									log.Printf("Skipping row due to invalid JSON in %s: %v\nData: %s\n", obj, err, strVal)
									valid = false
									break // stop checking this row
								}
							}
						}
						if valid {
							validRows = append(validRows, row)
						}
					}
					rowsSlice = validRows // overwrite with only valid rows
				}
				if table == "scene_markers" {
					for _, row := range rowsSlice {
						for _, tsKey := range []string{"created_at", "updated_at"} {
							if val, ok := row[tsKey]; ok {
								switch v := val.(type) {
								case string:
									t, err := time.Parse(time.RFC3339, v)
									if err != nil || !isValidPostgresTime(t) {
										log.Printf("Invalid time for %s: %v — using time.Now()", tsKey, val)
										row[tsKey] = time.Now().UTC()
									} else {
										row[tsKey] = t
									}
								case time.Time:
									if !isValidPostgresTime(v) {
										log.Printf("Out-of-range time for %s: %v — using time.Now()", tsKey, v)
										row[tsKey] = time.Now().UTC()
									}
								default:
									log.Printf("Unrecognized time format for %s: %v — using time.Now()", tsKey, val)
									row[tsKey] = time.Now().UTC()
								}
							}
						}
					}
				}

				q := dialect.Insert(table).Rows(rowsSlice)
				sql, args, err := q.ToSQL()
				if err != nil {
					return fmt.Errorf("failed tosql: %w", err)
				}

				_, err = dtxn.ExecContext(ctx, sql, args...)
				if err != nil {
					return fmt.Errorf("exec `%s` [%v]: %w", sql, args, err)
				}
			}

			// Move to the next batch
			offset += batchSize
		}
	}

	fmt.Printf("Setting sequences...\n")
	for _, table_name := range []string{
		"files", "folders", "galleries_chapters",
		"groups", "images", "performers",
		"saved_filters", "scene_markers",
		"scenes", "studios", "tags",
	} {
		sql := fmt.Sprintf(restart_seq, table_name)

		_, err = dtxn.ExecContext(ctx, sql)
		if err != nil {
			return fmt.Errorf("exec `%s`: %w", sql, err)
		}
	}

	if err := dtxn.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	if err := sourceDB.Close(); err != nil {
		return fmt.Errorf("source close: %w", err)
	}

	if err := destDB.Close(); err != nil {
		return fmt.Errorf("dest close: %w", err)
	}

	return nil
}

func main() {
	fmt.Println("postgres connector:")
	reader := bufio.NewReader(os.Stdin)
	pg_connector, err := reader.ReadString('\n')
	if err != nil {
		log.Fatal(err)
	}
	pg_connector = strings.TrimSpace(pg_connector)

	fmt.Println("sqlite db path:")
	reader = bufio.NewReader(os.Stdin)
	sqlite_path, err := reader.ReadString('\n')
	if err != nil {
		log.Fatal(err)
	}
	sqlite_path = strings.TrimSpace(sqlite_path)

	err = migrate(pg_connector, sqlite_path)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Migration successful!")
}
