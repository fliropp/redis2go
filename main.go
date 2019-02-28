package main

import (
	"fmt"
	"log"

	"github.com/garyburd/redigo/redis"
)

type SegmentPublicationArticle struct {
	Articleid string
	Score     float64
	Source    string
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
	pool = newPool()
}

/********************************
ORDINO READ
********************************/
func ordinoRead(publication string, segment string) (entry []SegmentPublicationArticle) {
	conn := pool.Get()
	defer conn.Close()

	list, err := redis.Strings(conn.Do("LRANGE", segment+publication, 0, 10))
	if err != nil {
		log.Fatal(err)
	}
	entries := []SegmentPublicationArticle{}
	for _, articleid := range list {
		data, err := redis.Values(conn.Do("HGETALL", segment+publication+articleid))
		if err != nil {
			log.Fatal(err)
		}
		entry := SegmentPublicationArticle{}
		err = redis.ScanStruct(data, &entry)
		if err != nil {
			log.Fatal(err)
		}
		entries = append(entries, entry)

	}
	return entries
}

/********************************
ORDINO WRITE
********************************/
func ordinoWrite(segment string, publication string, articleid string, score float64, source string) {
	conn := pool.Get()
	defer conn.Close()

	/*Check if key exists */
	exists, err := conn.Do("EXISTS", segment+publication+articleid)
	if err != nil {
		log.Fatal(err)
	}

	if exists.(int64) == 1 {
		resp, err := conn.Do("HGET", segment+publication+articleid, "Score")
		//existingScore := resp.Float64()
		if err != nil {
			log.Fatal(err)
		}
		update := resp //math.Max(resp, score)
		_, err = conn.Do("HMSET", "Score", update)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		/* Push new articleid to seg_pub list */
		//implement limit on list
		_, err := conn.Do("LPUSH", segment+publication, articleid)
		if err != nil {
			log.Fatal(err)
		}
		/* create record for new item in list */
		segPubArt := SegmentPublicationArticle{articleid, score, source}
		_, err = conn.Do("HMSET", redis.Args{segment + publication + articleid}.AddFlat(segPubArt)...)
		if err != nil {
			log.Fatal(err)
		}
	}

}

/********************************
MAIN
********************************/

func main() {
	ordinoWrite("age", "www.an.no", "133445", 0.6, "billboard")
	res := ordinoRead("www.an.no", "age")
	for _, entry := range res {
		fmt.Println(entry.Articleid)
		fmt.Println(entry.Source)
		fmt.Println(entry.Score)
	}
}
