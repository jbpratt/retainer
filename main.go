package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/bradfitz/iter"
	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	estransport "github.com/elastic/go-elasticsearch/v8/estransport"
	"github.com/tidwall/gjson"
)

var (
	index  string
	format string
	days   int
)

func init() {
	flag.StringVar(&index, "index", "", "index to retain")
	flag.IntVar(&days, "days", 1, "days to retain")
	flag.Parse()
}

func main() {

	if index == "" {
		log.Fatal(errors.New("must supply an index"))
	}

	cfg := elasticsearch.Config{
		Username: "elastic",
		Password: "changeme",
		Addresses: []string{
			"http://localhost:9200",
		},
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   10,
			ResponseHeaderTimeout: time.Second,
			DialContext:           (&net.Dialer{Timeout: time.Second}).DialContext,
		},
		Logger: &estransport.ColorLogger{Output: os.Stdout},
	}

	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("failed to create elasticsearch client: %v", err)
	}

	now := time.Now()

	indicies := []string{}
	scrollIDs := []string{}
	for i := range iter.N(days) {
		then := strings.ReplaceAll(now.AddDate(0, 0, -i).Format("2006-01-02"), "-", ".")
		name := fmt.Sprintf("%s-%s", index, then)
		indicies = append(indicies, name)
		// Perform initial search request to
		// get the first batch of data and the scroll ID
		//
		es.Indices.Refresh(es.Indices.Refresh.WithIndex(name))
		log.Println("Scrolling the indicies...")

		res, err := es.Search(
			es.Search.WithIndex(name),
			es.Search.WithSort("@timestamp"),
			es.Search.WithSize(10),
			es.Search.WithScroll(time.Minute),
		)
		if err != nil {
			log.Fatal(err)
		}
		if res.IsError() {
			log.Fatalf("error response: %s", res)
		}

		var b bytes.Buffer
		b.ReadFrom(res.Body)
		res.Body.Close()

		scrollID := gjson.Get(b.String(), "_scroll_id").String()
		scrollIDs = append(scrollIDs, scrollID)
	}

	// TODO: actually retain the logs
	// build list of scroll ids and process them
	var wg sync.WaitGroup
	for _, scrollID := range scrollIDs {
		wg.Add(1)
		go func(scrollID string) {
			for {
				res, err := es.Scroll(
					es.Scroll.WithScrollID(scrollID),
					es.Scroll.WithScroll(time.Minute),
				)
				if err != nil {
					log.Fatalf("failed to scroll: %v", err)
				}
				if res.IsError() {
					log.Fatalf("error response: %s", res)
				}

				var b bytes.Buffer
				b.ReadFrom(res.Body)
				res.Body.Close()

				hits := gjson.Get(b.String(), "hits.hits")

				if len(hits.Array()) < 1 {
					log.Println("Finished scrolling")
					wg.Done()
					break
				} else {
					fmt.Println(hits)
				}
			}
		}(scrollID)
	}

	wg.Wait()

	res, err := es.Indices.Delete(indicies)
	if err != nil {
		log.Fatal(fmt.Errorf("failed to delete indicies (%v): %v", indicies, err))
	}

	out, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Fatalf("failed to read body: %v", err)
	}

	fmt.Println(string(out))
}
