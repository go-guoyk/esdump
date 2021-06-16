package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/olivere/elastic"
	"io"
	"log"
	"os"
	"strings"
)

type WildcardQuery struct {
	src string
}

func (w WildcardQuery) Source() (interface{}, error) {
	var m map[string]interface{}
	err := json.Unmarshal([]byte(w.src), &m)
	return m, err
}

func main() {
	var err error
	defer func(err *error) {
		if *err != nil {
			log.Println("exited with error:", (*err).Error())
			os.Exit(1)
		} else {
			log.Println("exited")
		}
	}(&err)

	var (
		optURL         string
		optIndex       string
		optQuery       string
		optSort        string
		optScroll      string
		optMappingType string

		optSortDesc bool
	)

	flag.StringVar(&optIndex, "index", "logstash", "elasticsearch index to query")
	flag.StringVar(&optURL, "url", "http://127.0.0.1:9200", "elasticsearch connection url")
	flag.StringVar(&optQuery, "query", "", "elasticsearch query")
	flag.StringVar(&optMappingType, "mapping-type", "", "mapping type")
	flag.StringVar(&optScroll, "scroll", "", "scroll window")
	flag.StringVar(&optSort, "sort", "timestamp", "sort field")
	flag.Parse()

	if strings.HasPrefix(optSort, "-") {
		optSort = strings.TrimPrefix(optSort, "-")
		optSortDesc = true
	}

	var client *elastic.Client
	if client, err = elastic.NewClient(elastic.SetURL(strings.Split(optURL, ",")...), elastic.SetSniff(false)); err != nil {
		return
	}

	bs := client.Scroll(strings.Split(optIndex, ",")...)
	if optQuery != "" {
		bs = bs.Query(WildcardQuery{src: optQuery})
	}
	if optMappingType != "" {
		bs = bs.Type(strings.Split(optMappingType, ",")...)
	}
	if optScroll != "" {
		bs = bs.Scroll(optScroll)
	}
	bs = bs.Sort(optSort, !optSortDesc)

	var res *elastic.SearchResult
	for {
		if res, err = bs.Do(context.Background()); err != nil {
			if err == io.EOF {
				log.Println("found EOF")
				err = nil
				break
			} else {
				return
			}
		}
		for _, hit := range res.Hits.Hits {
			if hit.Source == nil {
				continue
			}
			fmt.Println(string(*hit.Source))
		}
	}
}
