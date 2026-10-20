package handlers

// Fixture-only SQL API corrections for the pinned upstream emulator. These use
// actual DuckDB definitions, never a hard-coded list of demo tables or keys.
import (
	"encoding/hex"
	"fmt"
	"github.com/nnnkkk7/snowflake-emulator/pkg/query"
	"github.com/nnnkkk7/snowflake-emulator/server/types"
	"regexp"
	"strings"
	"sync"
	"time"
)

var fixtureContext sync.Mutex
var keyQuery = regexp.MustCompile(`(?i)^SHOW (PRIMARY|UNIQUE|IMPORTED) KEYS IN SCHEMA "([^"]+)"\."([^"]+)"$`)

func fixtureSQL(sql string) string {
	for {
		sql = strings.TrimSpace(sql)
		if strings.HasPrefix(sql, "--") {
			if i := strings.IndexByte(sql, '\n'); i >= 0 {
				sql = sql[i+1:]
				continue
			}
		}
		if strings.HasPrefix(sql, "/*") {
			if i := strings.Index(sql, "*/"); i >= 0 {
				sql = sql[i+2:]
				continue
			}
		}
		break
	}
	if m := keyQuery.FindStringSubmatch(strings.TrimSpace(sql)); m != nil {
		predicate := "constraint_type = '" + map[string]string{"PRIMARY": "PRIMARY KEY", "UNIQUE": "UNIQUE", "IMPORTED": "FOREIGN KEY"}[strings.ToUpper(m[1])] + "'"
		filter := " database_name = '" + strings.ReplaceAll(m[2], "'", "''") + "' AND schema_name = '" + strings.ReplaceAll(m[3], "'", "''") + "' AND " + predicate
		if strings.EqualFold(m[1], "IMPORTED") {
			return `SELECT NULL::VARCHAR AS created_on, database_name AS pk_database_name, schema_name AS pk_schema_name, referenced_table AS pk_table_name, referenced_column_names[i] AS pk_column_name, database_name AS fk_database_name, schema_name AS fk_schema_name, table_name AS fk_table_name, constraint_column_names[i] AS fk_column_name, i AS key_sequence, 'NO ACTION' AS update_rule, 'NO ACTION' AS delete_rule, constraint_name AS fk_name, (SELECT p.constraint_name FROM duckdb_constraints() p WHERE p.database_name=k.database_name AND p.schema_name=k.schema_name AND p.table_name=k.referenced_table AND p.constraint_type IN ('PRIMARY KEY','UNIQUE') AND p.constraint_column_names=k.referenced_column_names LIMIT 1) AS pk_name FROM (SELECT *, generate_subscripts(constraint_column_names,1) AS i FROM duckdb_constraints()) k WHERE ` + filter
		}
		return `SELECT NULL::VARCHAR AS created_on, database_name, schema_name, table_name, constraint_column_names[i] AS column_name, i AS key_sequence, constraint_name FROM (SELECT *, generate_subscripts(constraint_column_names,1) AS i FROM duckdb_constraints()) k WHERE ` + filter
	}
	replacements := map[string]string{
		"schemata": `SELECT database_name AS catalog_name, schema_name FROM duckdb_schemas()`,
		"tables":   `SELECT database_name AS table_catalog, schema_name AS table_schema, table_name, 'BASE TABLE' AS table_type, comment FROM duckdb_tables() UNION ALL SELECT database_name, schema_name, view_name, 'VIEW', comment FROM duckdb_views() WHERE NOT internal`,
		"columns":  `SELECT database_name AS table_catalog, schema_name AS table_schema, table_name, column_name, column_index AS ordinal_position, CASE WHEN data_type LIKE 'DECIMAL%' OR data_type IN ('BIGINT','INTEGER','SMALLINT','HUGEINT','UBIGINT') THEN 'NUMBER' WHEN data_type='BLOB' THEN 'BINARY' WHEN data_type IN ('TIMESTAMP','TIMESTAMP_NS') THEN 'TIMESTAMP_NTZ' WHEN data_type='TIMESTAMP WITH TIME ZONE' THEN 'TIMESTAMP_TZ' ELSE data_type END AS data_type, CASE WHEN is_nullable THEN 'YES' ELSE 'NO' END AS is_nullable, column_default, comment, character_maximum_length, numeric_precision, numeric_scale, 'NO' AS is_identity FROM duckdb_columns()`,
		"views":    `SELECT database_name AS table_catalog, schema_name AS table_schema, view_name AS table_name, sql AS view_definition FROM duckdb_views() WHERE NOT internal`,
	}
	for name, replacement := range replacements {
		pattern := regexp.MustCompile(`(?i)\binformation_schema\.` + name + `\b`)
		sql = pattern.ReplaceAllString(sql, "("+replacement+")")
	}
	return sql
}
func fixtureData(result *query.Result) [][]interface{} {
	data := make([][]interface{}, len(result.Rows))
	for i, row := range result.Rows {
		data[i] = make([]interface{}, len(row))
		for j, value := range row {
			if value == nil {
				continue
			}
			var text string
			switch v := value.(type) {
			case time.Time:
				switch result.ColumnTypes[j].Type {
				case "DATE":
					text = v.Format("2006-01-02")
				case "TIME":
					text = v.Format("15:04:05.000000000")
				case "TIMESTAMP_TZ", "TIMESTAMP_LTZ":
					text = v.UTC().Format("2006-01-02 15:04:05.000000000 -07:00")
				default:
					text = v.Format("2006-01-02 15:04:05.000000000")
				}
			default:
				text = fmt.Sprint(value)
				if result.ColumnTypes[j].Type == "BINARY" {
					text = hex.EncodeToString([]byte(text))
				}
			}
			data[i][j] = text
		}
	}
	return data
}
func fixtureTypes(fields []types.RowTypeField) {
	for i := range fields {
		if fields[i].Type == "NUMBER" || fields[i].Type == "FIXED" {
			fields[i].Type = "fixed"
			if fields[i].Precision == 0 {
				fields[i].Precision = 38
			}
		}
	}
}
