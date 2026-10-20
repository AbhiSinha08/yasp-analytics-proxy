"""Apply narrow, version-pinned emulator corrections during the Docker build."""
from pathlib import Path
p=Path('server/handlers/rest_api_v2.go');s=p.read_text()
s=s.replace('// Create statement record', '''fixtureContext.Lock()
    defer fixtureContext.Unlock()
    if req.Schema != "" {
        _, err := h.executor.Execute(r.Context(), "SET schema = '" + strings.ReplaceAll(req.Database+"."+req.Schema, "'", "''") + "'")
        if err != nil { h.sendError(w, 400, err.Error(), types.SQLState42000); return }
    }
    req.Statement = fixtureSQL(req.Statement)
    // Create statement record''',1)
s=s.replace('"net/http"','"net/http"\n "strings"')
s=s.replace('data := make([][]interface{}, len(result.Rows))\n\tcopy(data, result.Rows)','fixtureTypes(rowType)\n data := fixtureData(result)')
p.write_text(s)
p=Path('cmd/server/main.go');s=p.read_text().replace('connMgr := connection.NewManager(db)', 'db.SetMaxOpenConns(1)\n connMgr := connection.NewManager(db)');p.write_text(s)
# The upstream MySQL printer corrupts PostgreSQL double quotes and catalog names.
# Keep DuckDB-supported expressions intact rather than pretty-printing them.
p=Path('pkg/query/translator.go');s=p.read_text();a=s.index('func (t *Translator) Translate(');b=s.index('// handleComplexTransformations',a)
s=s[:a]+'''func (t *Translator) Translate(sql string) (string, error) {
    return translateTypes(sql), nil
}

'''+s[b:];p.write_text(s)
# database/sql does not expose DecimalSize for the pinned DuckDB driver.
p=Path('pkg/query/type_mapper.go');s=p.read_text().replace('"strings"','"strings"\n "fmt"');s=s.replace('meta.Type = m.MapDuckDBType(dbType)', '''meta.Type = m.MapDuckDBType(dbType)
                if strings.HasPrefix(dbType,"DECIMAL(") { fmt.Sscanf(dbType,"DECIMAL(%d,%d)",&meta.Precision,&meta.Scale) }
                if meta.Type == "NUMBER" && meta.Precision==0 { meta.Precision=38 }''');p.write_text(s)

# Read-only CTEs are queries too; Exec would discard their result set.
p=Path('pkg/query/classifier.go');s=p.read_text().replace('return strings.HasPrefix(upperSQL, "SELECT") ||', 'return strings.HasPrefix(upperSQL, "WITH") || strings.HasPrefix(upperSQL, "SELECT") ||');p.write_text(s)
