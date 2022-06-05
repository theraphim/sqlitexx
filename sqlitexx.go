package sqlitexx

import (
	"context"
	"math"
	"time"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

func PooledExecute(ctx context.Context, pool *sqlitex.Pool, query string, prepFn func(*sqlite.Stmt), resultFn func(*sqlite.Stmt) error) error {
	conn := pool.Get(ctx)
	if conn == nil {
		return ctx.Err()
	}
	defer pool.Put(conn)

	return JustExec(conn, query, prepFn, resultFn)
}

func JustExec(conn *sqlite.Conn, query string, prepFn func(*sqlite.Stmt), resultFn func(*sqlite.Stmt) error) error {
	var stmt *sqlite.Stmt
	var err error
	stmt, err = conn.Prepare(query)
	if err != nil {
		return err
	}
	if prepFn != nil {
		prepFn(stmt)
	}
	err = execLoop(stmt, resultFn)
	resetErr := stmt.Reset()
	if err == nil {
		err = resetErr
	}
	return err
}

func execLoop(stmt *sqlite.Stmt, resultFn func(stmt *sqlite.Stmt) error) error {
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			return nil
		}
		if resultFn == nil {
			continue
		}
		if err := resultFn(stmt); err != nil {
			return err
		}
	}
}

func ToSQLiteTime(t time.Time) float64 {
	return ((float64(t.UnixNano()) / float64(time.Second)) / 86400.0) + 2440587.5
}

func FromSQLiteTime(f float64) time.Time {
	nt := int64(math.Round((f - 2440587.5) * 86400.0 * 1000))
	return time.Unix(0, nt*1000*1000)
}

func StmtGetBytesName(stmt *sqlite.Stmt, index string) []byte {
	n := stmt.GetLen(index)
	if n == 0 {
		return nil
	}
	result := make([]byte, n)
	stmt.GetBytes(index, result)
	return result
}
