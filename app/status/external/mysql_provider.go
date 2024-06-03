package external

import (
	"context"
	"database/sql"
	"log"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// MysqlProvider is a status provider that uses mysql
type MysqlProvider struct {
	TimeOut time.Duration
}

// Status returns status of mysql, checks if connection established
func (m *MysqlProvider) Status(req Request) (*Response, error) {
	st := time.Now()
	log.Println("mysql provider for ", req.URL)
	ctx, cancel := context.WithTimeout(context.Background(), m.TimeOut)
	defer cancel()

	// Connect to mysql
	u := strings.TrimPrefix(req.URL, "mysql://")
	db, err := sql.Open("mysql", u)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Ping mysql
	err = db.PingContext(ctx)
	if err != nil {
		return nil, err
	}

	// Get seconds behind master
	secondsBehindMaster, err := getSecondsBehindMaster(db)
	if err != nil {
		result := Response{
			Name:         req.Name,
			StatusCode:   200,
			Body:         map[string]interface{}{"status": "error", "seconds_behind_master": -1},
			ResponseTime: time.Since(st).Milliseconds(),
		}
		return &result, nil
	}

	result := Response{
		Name:         req.Name,
		StatusCode:   200,
		Body:         map[string]interface{}{"status": "ok", "seconds_behind_master": secondsBehindMaster},
		ResponseTime: time.Since(st).Milliseconds(),
	}
	return &result, nil
}

func getSecondsBehindMaster(db *sql.DB) (int, error) {
	var secondsBehindMaster int

	rows, err := db.Query("SHOW SLAVE STATUS")
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return 0, err
	}

	values := make([]sql.RawBytes, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range columns {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return 0, err
		}
		for i, col := range columns {
			if col == "Seconds_Behind_Master" {
				if values[i] != nil {
					secondsBehindMaster, err = strconv.Atoi(string(values[i]))
					if err != nil {
						return 0, err
					}
				}
			}
		}
	}

	return secondsBehindMaster, nil
}
