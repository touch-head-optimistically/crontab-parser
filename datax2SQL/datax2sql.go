//
// Creating Time: 2018.05
//
// Message: serve for DataX conf

package access

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "gopkg.in/rana/ora.v4"
)

// Conf2SQL inputs a DataX conf then returns a create table sql string.
func Conf2SQL(conf string) (rs source.SQLParsedRes, err error) {
	var cfgJSON source.DataXConfInfo
	err = json.Unmarshal([]byte(conf), &cfgJSON)
	if err != nil {
		logger.Error.Println(err)
		return
	}

	for _, v := range cfgJSON.Job.Content {
		var rowMaps []map[string]interface{}

		// reader: gets colmaps from source database
		switch v.Reader.Name {
		case source.MySQLReader, source.OracleReader,
			source.SQLserverReader, source.PostSQLReader:
			rowMaps, err = fetchReaderCols(cfgJSON)
			if err != nil {
				logger.Error.Println(err)
				return rs, err
			}

		default:
			err := errors.New("unsupported reader database")
			logger.Info.Println(err)
			return rs, err
		}

		// writer: extracts create_table SQL
		switch v.Writer.Name {
		case source.MySQLWriter, source.OracleWriter,
			source.SQLserverWriter, source.PostSQLWriter:

			err = fetchWriterSQL(rowMaps, &rs, cfgJSON)
			if err != nil {
				logger.Error.Println(err)
				return rs, err
			}

			rs.WriterName = v.Writer.Name
			rs.WriterUser = v.Writer.Parameter.Username
			rs.WriterPassword = v.Writer.Parameter.Password

		default:
			err := errors.New("unsupported writer database")
			logger.Info.Println(err)
			return rs, err
		}
	}
	return rs, err
}

// SubmitSQL submits a new query to database(datax writer name).
func SubmitSQL(s source.SubmitSQLJSON) error {
	var db *sql.DB
	var err error

	var info source.DatabaseInfo
	info.IP = s.IP
	info.Port = s.Port
	info.Database = s.Database
	info.User = s.User
	info.Password = s.Password

	switch s.WriterName {
	case source.PostSQLWriter:
		db, err = getPGcnn(info)
		defer db.Close()
		if err != nil {
			logger.Error.Println(err)
		}
	case source.SQLserverWriter:
		db, err = getSQScnn(info)
		defer db.Close()
		if err != nil {
			logger.Error.Println(err)
		}
	case source.MySQLWriter:
		db, err = getMSScnn(info)
		defer db.Close()
		if err != nil {
			logger.Error.Println(err)
		}
	case source.OracleWriter:
		db, err = getORAcnn(info)
		defer db.Close()
		if err != nil {
			logger.Error.Println(err)
		}
	}

	_, err = db.Query(s.SQL)
	if err != nil {
		logger.Error.Println(err)
		return err
	}
	return nil
}

// fetchReaderCols returns table's schema in maps.
func fetchReaderCols(dx source.DataXConfInfo) (colmaps []map[string]interface{}, err error) {
	for _, v := range dx.Job.Content {
		var dbInfo source.DatabaseInfo

		dbInfo.User = v.Reader.Parameter.Username
		dbInfo.Password = v.Reader.Parameter.Password

		for _, c := range v.Reader.Parameter.Connection {
			var passed bool
			for _, j := range c.JdbcURL {
				// until find a valid jdbcurl
				switch v.Reader.Name {
				case source.MySQLReader, source.PostSQLReader:
					// jdbc:mysql://127.0.0.1:3306/database
					ipd := strings.Split(j, "jdbc:mysql://")[1]
					dbInfo.IP = strings.Split(ipd, ":")[0]
					dbInfo.Port, _ = strconv.Atoi(strings.Split(strings.Split(ipd, "/")[0], ":")[1])
					dbInfo.Database = strings.Split(ipd, "/")[1]

				case source.OracleReader:
					// jdbc:oracle:thin:@[HOST_NAME]:PORT:[DATABASE_NAME] (for oracle)
					ipd := strings.Split(j, "jdbc:oracle:thin:@")[1]
					dbInfo.IP = strings.Split(ipd, ":")[0]
					dbInfo.Port, _ = strconv.Atoi(strings.Split(ipd, ":")[1])
					dbInfo.Database = strings.Split(ipd, ":")[2]

				case source.SQLserverReader:
					// jdbc:sqlserver://localhost:3433;DatabaseName=dbname (for sqlserver)
					ipd := strings.Split(j, "dbc:sqlserver://")[1]
					dbInfo.IP = strings.Split(ipd, ":")[0]
					dbInfo.Port, _ = strconv.Atoi(strings.Split(strings.Split(ipd, ":")[1], ";")[0])
					for _, x := range strings.Split(ipd, ";") {
						if strings.Split(x, "=")[0] == "DatabaseName" {
							dbInfo.Database = strings.Split(x, "=")[1]
						}
					}
				}

				switch v.Reader.Name {
				case source.MySQLReader:
					colmaps, err = MSSCol2SQL(dbInfo, c.Table[0])
				case source.OracleReader:
					colmaps, err = ORACol2SQL(dbInfo, c.Table[0])
				case source.SQLserverReader:
					colmaps, err = SQSCol2SQL(dbInfo, c.Table[0])
				case source.PostSQLReader:
					colmaps, err = PGCol2SQL(dbInfo, "public", c.Table[0])
				}
				passed = true
			}

			if !passed {
				err := errors.New("failed to get source table columns: all jdbcurls are inaccessible.")
				logger.Info.Println(err)
				return colmaps, err
			}
		}
	}

	return colmaps, err
}

// fetchWriterSQL gets table_create SQL with rowmaps.
func fetchWriterSQL(rowMaps []map[string]interface{}, rs *source.SQLParsedRes, dx source.DataXConfInfo) error {
	var destTables []string

	for _, c := range dx.Job.Content {
		for _, cnn := range c.Writer.Parameter.Connection {
			rs.WriterJDBCURLs = cnn.JdbcURL
			destTables = cnn.Table

			switch c.Writer.Name {
			case source.MySQLWriter, source.PostSQLWriter:
				// jdbc:mysql://127.0.0.1:3306/database
				rs.WriterDatabase = strings.Split(strings.Split(strings.Split(cnn.JdbcURL, "//")[1], "/")[1], "?")[0]
				rs.WriterIP = strings.Split(strings.Split(strings.Split(cnn.JdbcURL, "//")[1], "/")[0], ":")[0]
				rs.WriterPort, _ = strconv.Atoi(strings.Split(strings.Split(strings.Split(cnn.JdbcURL, "//")[1], "/")[0], ":")[1])
			case source.OracleWriter:
				// jdbc:oracle:thin:@[HOST_NAME]:PORT:[DATABASE_NAME] (for oracle)
				rs.WriterDatabase = strings.Split(strings.Split(cnn.JdbcURL, "jdbc:oracle:thin:@")[1], ":")[2]
				rs.WriterIP = strings.Split(strings.Split(cnn.JdbcURL, "jdbc:oracle:thin:@")[1], ":")[0]
				rs.WriterPort, _ = strconv.Atoi(strings.Split(strings.Split(cnn.JdbcURL, "jdbc:oracle:thin:@")[1], ":")[1])
			case source.SQLserverWriter:
				// jdbc:sqlserver://localhost:3433;DatabaseName=dbname;xx=xx;
				for k, x := range strings.Split(strings.Split(cnn.JdbcURL, "jdbc:sqlserver://")[1], ";") {
					if k == 0 {
						rs.WriterIP = strings.Split(x, ":")[0]
						rs.WriterPort, _ = strconv.Atoi(strings.Split(x, ":")[1])
					}
					if strings.Split(x, "=")[0] == "DatabaseName" {
						rs.WriterDatabase = strings.Split(x, "=")[1]
					}
				}
			default:
				// not supported
			}
		}
	}

	//多表循环,生成多条建表语句
	for k, v := range destTables {
		var colString string
		for k, cols := range rowMaps {
			var col string
			if k != len(rowMaps)-1 {
				if cols["col"] != nil {
					col = fmt.Sprintf("%s %s,\n", cols["col"], cols["type"])
				} else {
					col = fmt.Sprintf("%s %s,\n", cols["COL"], cols["TYPE"])
				}
			} else {
				if cols["col"] != nil {
					col = fmt.Sprintf("%s %s\n", cols["col"], cols["type"])
				} else {
					col = fmt.Sprintf("%s %s\n", cols["COL"], cols["TYPE"])
				}
			}

			colString = colString + col
		}
		var tableString = fmt.Sprintf("CREATE TABLE %s(\n%s)", v, colString)
		if k == 0 {
			rs.SQL = tableString
		} else {
			rs.SQL = rs.SQL + ";\n" + tableString
		}
	}

	return nil
}

// getPGcnn gets PostgreSQL connection.
func getPGcnn(info source.DatabaseInfo) (*sql.DB, error) {
	var db *sql.DB
	pgLink := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		info.User,
		info.Password,
		info.IP,
		info.Port,
		info.Database)
	db, err := sql.Open("postgres", pgLink)
	if err != nil {
		logger.Error.Println(err)
		db.Close()
		return db, err
	}

	return db, nil
}

// getMSScnn gets MySQL connection.
func getMSScnn(info source.DatabaseInfo) (*sql.DB, error) {
	var db *sql.DB
	link := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", info.User, info.Password, info.IP, info.Port, info.Database)

	fmt.Println(link)
	db, err := sql.Open("mysql", link)
	if err != nil {
		logger.Error.Println(err)
		db.Close()
		return db, err
	}

	return db, nil
}

// getSQScnn gets SQLserver connection.
func getSQScnn(info source.DatabaseInfo) (*sql.DB, error) {
	var db *sql.DB
	link := fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s",
		info.User, info.Password, info.IP, info.Port, info.Database)

	fmt.Println(link)
	db, err := sql.Open("mssql", link)
	if err != nil {
		logger.Error.Println(err)
		db.Close()
		return db, err
	}
	return db, nil
}

// getORAcnn gets Oracle connection.
func getORAcnn(info source.DatabaseInfo) (*sql.DB, error) {
	var db *sql.DB

	link := fmt.Sprintf("%s/%s@%s:%d/%s",
		info.User, info.Password, info.IP, info.Port, info.Database)

	fmt.Println(link)
	db, err := sql.Open("ora", link)
	if err != nil {
		logger.Error.Println(err)
		db.Close()
		return db, err
	}

	return db, nil
}

// MSSCol2SQL extracts columns to create table SQL string.
func MSSCol2SQL(info source.DatabaseInfo, table string) ([]map[string]interface{}, error) {
	var rowMaps []map[string]interface{}

	db, err := getMSScnn(info)
	defer db.Close()
	if err != nil {
		logger.Error.Println(err)
		return rowMaps, err
	}

	sqltxt := fmt.Sprintf(`select column_name as col, data_type as type
							from information_schema.columns
							where table_name = '%s'`, table)
	fmt.Println(sqltxt)
	rows, err := db.Query(sqltxt)
	if err != nil {
		logger.Error.Println(err)
		return rowMaps, err
	}
	defer rows.Close()

	rowMaps, err = sqlrows2Maps(rows)
	if err != nil {
		logger.Error.Println(err)
		return rowMaps, err
	}

	return rowMaps, nil
}

// PGCol2SQL extracts columns to create table SQL string.
func PGCol2SQL(info source.DatabaseInfo, schemaname string, table string) ([]map[string]interface{}, error) {
	var rowMaps []map[string]interface{}

	db, err := getPGcnn(info)
	defer db.Close()
	if err != nil {
		logger.Error.Println(err)
		return rowMaps, err
	}

	sqltxt := fmt.Sprintf(
		`select column_name as col, data_type as type
		 from information_schema.columns
		 where table_name = '%s' and table_schema = '%s'`,
		table, schemaname)

	fmt.Println(sqltxt)
	rows, err := db.Query(sqltxt)
	if err != nil {
		logger.Error.Println(err)
		return rowMaps, err
	}
	defer rows.Close()

	rowMaps, err = sqlrows2Maps(rows)
	if err != nil {
		logger.Error.Println(err)
		return rowMaps, err
	}

	return rowMaps, nil
}

// SQSCol2SQL extracts columns to create table SQL string.
func SQSCol2SQL(info source.DatabaseInfo, table string) ([]map[string]interface{}, error) {
	var rowMaps []map[string]interface{}

	db, err := getSQScnn(info)
	defer db.Close()
	if err != nil {
		logger.Error.Println(err)
		return rowMaps, err
	}

	sqltxt := fmt.Sprintf(
		`select column_name as col, data_type as type
		 from information_schema.columns
	     where table_name = '%s'`, table)
	fmt.Println(sqltxt)
	rows, err := db.Query(sqltxt)
	if err != nil {
		logger.Error.Println(err)
		return rowMaps, err
	}
	defer rows.Close()

	rowMaps, err = sqlrows2Maps(rows)
	if err != nil {
		logger.Error.Println(err)
		return rowMaps, err
	}

	return rowMaps, nil
}

// ORACol2SQL extracts columns to create table SQL string.
func ORACol2SQL(info source.DatabaseInfo, table string) ([]map[string]interface{}, error) {
	var rowMaps []map[string]interface{}

	db, err := getORAcnn(info)
	defer db.Close()
	if err != nil {
		logger.Error.Println(err)
		return rowMaps, err
	}

	if strings.Contains(table, ".") {
		table = strings.Split(table, ".")[1]
	}
	sqltxt := fmt.Sprintf(
		`select column_name as col, data_type as type
		 from all_tab_cols
	     where table_name = '%s'`,
		table)

	fmt.Println(sqltxt)
	rows, err := db.Query(sqltxt)
	if err != nil {
		logger.Error.Println(err)
		return rowMaps, err
	}
	defer rows.Close()

	rowMaps, err = sqlrows2Maps(rows)
	if err != nil {
		logger.Error.Println(err)
		return rowMaps, err
	}

	return rowMaps, nil
}

//sqlrows2Maps extracts table's rows to map.
func sqlrows2Maps(rws *sql.Rows) ([]map[string]interface{}, error) {
	var rowMaps []map[string]interface{}

	var columns []string
	columns, err := rws.Columns()
	if err != nil {
		logger.Error.Println(err)
		return rowMaps, err
	}

	values := make([]sql.RawBytes, len(columns))
	scans := make([]interface{}, len(columns))
	for i := range values {
		scans[i] = &values[i]
	}

	for rws.Next() {
		_ = rws.Scan(scans...)
		each := map[string]interface{}{}
		for i, col := range values {
			each[columns[i]] = string(col)
		}
		rowMaps = append(rowMaps, each)
	}
	return rowMaps, nil
}
