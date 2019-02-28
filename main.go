package main

import (
	"fmt"
	"log"

	"github.com/garyburd/redigo/redis"
)

type OrdinoDataEntry struct {
	Ordval      float64
	Artid       string
	Segment     string
	Publication string
	Source      string
}

var pool *redis.Pool

func newPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle:   80,
		MaxActive: 12000, // max number of connections
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", ":6379")
			if err != nil {
				panic(err.Error())
			}
			return c, err
		},
	}
}

func init() {
	fmt.Println("init...")
	pool = newPool()
}

func ordinoGet() OrdinoDataEntry {
	conn := pool.Get()
	defer conn.Close()

	data, err := redis.Values(conn.Do("HGETALL", "ordinoData:1"))
	if err != nil {
		log.Fatal(err)
	}
	entry := OrdinoDataEntry{}
	err = redis.ScanStruct(data, &entry)
	if err != nil {
		log.Fatal(err)
	}
	return entry
}

func ordinoSet() {
	conn := pool.Get()
	defer conn.Close()

	ordinoDataEntry := OrdinoDataEntry{0.5, "1234567", "age", "www.an.no", "billboard"}
	_, err := conn.Do("HMSET", redis.Args{"ordinoData:1"}.AddFlat(ordinoDataEntry)...)
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	ordinoSet()
	//entry := OrdinoDataEntry{}
	entry := ordinoGet()
	//entry, err := populateOrdinoEntry(res)
	fmt.Println(entry.Ordval)
	fmt.Println(entry.Segment)
	fmt.Println(entry.Artid)
	fmt.Println(entry.Publication)
	fmt.Println(entry.Source)

}
