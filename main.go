package main

import (
	"database/sql/driver"
	"fmt"
	"github.com/google/uuid"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go"
	data "github.com/ClickHouse/clickhouse-go/lib/data"
)

func main() {
	//tcp://localhost:9000?username=default&password=&database=default
	uuid.New()
	uuid.GetTime()
	for i := 0; i < 100; i++ {
		fmt.Println("===================  IIII")
		execInseart()
	}
}

func execInseart() {
	connect, err := clickhouse.OpenDirect("tcp://127.0.0.1:9000?username=&debug=true&compress=1")
	if err != nil {
		log.Fatal(err)
	}
	{
		connect.Begin()
		stmt, _ := connect.Prepare(`
			CREATE TABLE IF NOT EXISTS example (
				uuid   String,
				utm_source   String,
				utm_medium   String,
				utm_campaign String,
				utm_term     String,
				utm_content  String,
				cms_user_id  UInt32,
				seo_user     String,
				event_date Date DEFAULT toDate(now()),
				event_time DateTime,
				PRIMARY KEY (event_date, event_time, uuid)
				) ENGINE = MergeTree()
				PARTITION BY toYYYYMM(event_date)
				ORDER BY (event_date, event_time, uuid);
		`)

		if _, err := stmt.Exec([]driver.Value{}); err != nil {
			log.Fatal(err)
		}

		if err := connect.Commit(); err != nil {
			log.Fatal(err)
		}
	}
	{
		connect.Begin()
		connect.Prepare("INSERT INTO example " +
			"(utm_source, uuid, event_time, event_date) VALUES " +
			"(?, ?, ?, ?) ")

		block, err := connect.Block()
		if err != nil {
			log.Fatal(err)
		}

		blocks := []*data.Block{block, block.Copy()}

		var wg sync.WaitGroup
		wg.Add(len(blocks))

		for i := range blocks {
			b := blocks[i]
			go func() {
				defer wg.Done()
				writeBatch(b, 5000000)
				if err := connect.WriteBlock(b); err != nil {
					log.Fatal(err)
				}
			}()
		}

		wg.Wait()

		if err := connect.Commit(); err != nil {
			log.Fatal(err)
		}
	}
	{
		connect.Begin()
		stmt, _ := connect.Prepare(`SELECT count() FROM example`)

		rows, err := stmt.Query([]driver.Value{})
		if err != nil {
			log.Fatal(err)
		}

		columns := rows.Columns()
		row := make([]driver.Value, 1)
		for rows.Next(row) == nil {
			for i, c := range columns {
				log.Print(c, " :^^^^^^^^^^^^^6^^^^^^^^^ ", row[i])
			}
		}

		if err := connect.Commit(); err != nil {
			log.Fatal(err)
		}
	}
	{
		// connect.Begin()
		//stmt, _ := connect.Prepare(`DROP TABLE example`)
		//if _, err := stmt.Exec([]driver.Value{}); err != nil {
		//	log.Fatal(err)
		//}
		//if err := connect.Commit(); err != nil {
		//	log.Fatal(err)
		//}
	}
}

func writeBatch(block *data.Block, n int) {
	block.Reserve()
	block.NumRows += uint64(n)

	//for i := 0; i < n; i++ {
	//	block.WriteUInt8(0, uint8(10+i))
	//}
	//
	//for i := 0; i < n; i++ {
	//	block.WriteDate(1, time.Now())
	//}
	//
	//for i := 0; i < n; i++ {
	//	block.WriteArray(2, clickhouse.Array([]string{"A", "B", "C"}))
	//}
	//
	//for i := 0; i < n; i++ {
	//	block.WriteArray(3, clickhouse.Array([]uint8{1, 2, 3, 4, 5}))
	//}

	utm_sources := make([]string, 5)
	utm_sources[0] = "utm_sources_aaaaaaa"
	utm_sources[1] = "utm_sources_ooooooo"
	utm_sources[2] = "utm_sources_kkkkkkk"
	for i := 0; i < n; i++ {
		min := 0
		max := 2
		k := rand.Intn(max-min) + min
		block.WriteString(0, utm_sources[k])
	}

	for i := 0; i < n; i++ {
		id := uuid.NewString()
		//id := uuid.New()
		block.WriteString(1, id)
	}

	for i := 0; i < n; i++ {
		t := randate()
		block.WriteDateTime(2, t)
		block.WriteDate(3, t)
	}
}

//func generateData () {
//
//}

func randate() time.Time {
	min := time.Date(2021, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Date(2023, 5, 2, 0, 0, 0, 0, time.UTC).Unix()
	delta := max - min

	sec := rand.Int63n(delta) + min
	return time.Unix(sec, 0)
}
