package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/parquet-go/parquet-go"
)

type Page struct {
	Id         int64
	Namespace  int64
	Title      string
	IsRedirect bool
	IsNew      bool
	Latest     int64
}

type PageLink struct {
	FromId        int64
	FromNamespace int64
	ToNamespace   int64
	ToTitle       string
}

type Redirect struct {
	From      int64
	Namespace int64
	Title     string
	Interwiki string
}

func convertTable[T interface{}](name string, date string, schema *T, parser func(line string) [](T)) error {
	filenameIn := "data\\frwiki-" + date + "-" + name + ".sql.gz"
	filenameOut := "data\\frwiki-" + date + "-" + name + ".parquet"
	fileIn, err := os.Open(filenameIn)

	headerLine := "INSERT INTO `" + name + "` VALUES"

	if err != nil {
		return err
	}
	defer fileIn.Close()

	reader, err := gzip.NewReader(fileIn)

	if err != nil {
		return err
	}
	defer reader.Close()

	bufferedReader := bufio.NewReader(reader)

	// Create a new Parquet file writer
	// The file will be written to the specified io.Writer
	// with the given Arrow schema and Parquet configuration
	ch := make(chan [](T), 1000000)
	var wg sync.WaitGroup
	wg.Add(1)
	go write(ch, filenameOut, schema, &wg)

	// string that contains all the insert statements
	lineBuffer := ""
	keepReading := false
	for {
		line, isPrefix, err := bufferedReader.ReadLine()
		if err != nil {
			if err.Error() == "EOF" {
				break
			} else {
				return err
			}
		}

		lineString := string(line)
		newKeepReading := keepReading
		if strings.HasPrefix(lineString, headerLine) {
			newKeepReading = true
		}
		if newKeepReading {
			lineBuffer += lineString
		}
		if strings.HasSuffix(lineString, ";") {
			newKeepReading = false
		}
		keepReading = newKeepReading
		if !isPrefix && !keepReading && lineBuffer != "" {
			buf := parser(lineBuffer)
			ch <- buf
			lineBuffer = ""
		}

	}
	close(ch)
	wg.Wait()
	log.Println("Write Finished")
	return nil
}

func parseSqlRedirect(line string) []Redirect {
	regex := regexp.MustCompile(`\((?P<from>\d+),(?P<namespace>\d+),'(?P<title>(?:\\.|[^\\'])*)',(?P<interwiki>'((?:\\.|[^\\'])*)'|NULL),'((?:\\.|[^\\'])*)'\)`)
	fromIndex := regex.SubexpIndex("from")
	namespaceIndex := regex.SubexpIndex("namespace")
	titleIndex := regex.SubexpIndex("title")
	interwikiIndex := regex.SubexpIndex("interwiki")
	matches := regex.FindAllStringSubmatch(line, -1)
	redirects := make([]Redirect, 0, len(matches))
	for _, match := range matches {
		from, err := strconv.Atoi(match[fromIndex])
		if err != nil {
			panic(err)
		}
		namespace, err := strconv.Atoi(match[namespaceIndex])
		if err != nil {
			panic(err)
		}
		if match[interwikiIndex] == "NULL" {
			log.Panicln("Interwiki is null", match)
		}
		redirect := Redirect{
			From:      int64(from),
			Namespace: int64(namespace),
			Title:     unquote(match[titleIndex]),
			Interwiki: unquote(match[interwikiIndex]),
		}
		redirects = append(redirects, redirect)
	}
	return redirects
}

func parseSqlPage(line string) []Page {
	regex := regexp.MustCompile(`(?i)\((?P<id>\d+),(?P<namespace>\d+),'(?P<title>(?:\\.|[^\\'])*)',(?P<isRedirect>\d),(?P<isNew>\d),([\d\.]+),'((?:\\.|[^\\'])*)',('((?:\\.|[^\\'])*)'|NULL),(?P<latest>\d+),(\d+),('((?:\\.|[^\\'])*)'|NULL),('((?:\\.|[^\\'])*)'|NULL)\)`)
	idIndex := regex.SubexpIndex("id")
	namespaceIndex := regex.SubexpIndex("namespace")
	titleIndex := regex.SubexpIndex("title")
	isRedirectIndex := regex.SubexpIndex("isRedirect")
	isNewIndex := regex.SubexpIndex("isNew")
	latestIndex := regex.SubexpIndex("latest")

	matches := regex.FindAllStringSubmatch(line, -1)
	pages := make([]Page, 0, len(matches))
	for _, match := range matches {
		id, err := strconv.Atoi(match[idIndex])
		if err != nil {
			panic(err)
		}
		namespace, err := strconv.Atoi(match[namespaceIndex])
		if err != nil {
			panic(err)
		}
		latest, err := strconv.Atoi(match[latestIndex])
		if err != nil {
			panic(err)
		}

		page := Page{
			Id:         int64(id),
			Namespace:  int64(namespace),
			Title:      unquote(match[titleIndex]),
			IsRedirect: match[isRedirectIndex] == "1",
			IsNew:      match[isNewIndex] == "1",
			Latest:     int64(latest),
		}
		pages = append(pages, page)
	}
	return pages
}

func parseSqlPageLinks(line string) []PageLink {
	regex := regexp.MustCompile(`\((?P<from>\d+),(?P<namespace>\d+),'(?P<title>(?:\\.|[^\\'])*)',(?P<fromNamespace>\d+)\)`)
	fromIdIndex := regex.SubexpIndex("from")
	toNamespaceIndex := regex.SubexpIndex("namespace")
	toTitleIndex := regex.SubexpIndex("title")
	fromNamespaceIndex := regex.SubexpIndex("fromNamespace")

	matches := regex.FindAllStringSubmatch(line, -1)
	pageLinks := make([]PageLink, 0, len(matches))
	for _, match := range matches {
		fromId, err := strconv.Atoi(match[fromIdIndex])
		if err != nil {
			panic(err)
		}
		toNamespace, err := strconv.Atoi(match[toNamespaceIndex])
		if err != nil {
			panic(err)
		}
		fromNamespace, err := strconv.Atoi(match[fromNamespaceIndex])
		if err != nil {
			panic(err)
		}

		pageLink := PageLink{
			FromId:        int64(fromId),
			FromNamespace: int64(fromNamespace),
			ToTitle:       unquote(match[toTitleIndex]),
			ToNamespace:   int64(toNamespace),
		}
		pageLinks = append(pageLinks, pageLink)
	}
	return pageLinks
}

func write[T interface{}](ch chan [](T), fileName string, schema *T, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Println("Write Started")
	f, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	schemaParquet := parquet.SchemaOf(schema)
	writer := parquet.NewGenericWriter[T](f, schemaParquet, &parquet.WriterConfig{Compression: &parquet.Snappy})
	if err != nil {
		panic(err)
	}
	defer writer.Close()
	iter := 0
	for {
		select {
		case buf, ok := <-ch:
			{
				iter++
				if !ok {
					fmt.Println("Channel closed, exiting")
					return
				}
				if len(buf) == 0 {
					continue
				}
				nb, err := writer.Write(buf)
				if err != nil {
					panic(err)
				}
				if (iter % 30) == 0 {
					fmt.Println("Wrote", nb, "rows")
				}
			}

		}
	}
}

func unquote(input string) string {
	if input == "''" {
		return ""
	}
	return strings.ReplaceAll(input, "\\'", "'")

}

func main() {

	startTime := time.Now()
	// err := convertTable("redirect", "20230801", new(Redirect), parseSqlRedirect)
	err := convertTable("page", "20230801", new(Page), parseSqlPage)
	// err := convertTable("pagelinks", "20230801", new(PageLink), parseSqlPageLinks)
	if err != nil {
		fmt.Println(err)
	}
	log.Println("Done in ", time.Since(startTime))
	// errChan := make(chan error)
	// var wg sync.WaitGroup

	// wg.Add(3)
	// go func() {
	// 	defer wg.Done()
	// 	err := convertTable("pagelinks", "20230801", pageLinkSchema, parseSqlPageLinks)
	// 	if err != nil {
	// 		errChan <- err
	// 	}
	// }()
	// go func() {
	// 	defer wg.Done()
	// 	err := convertTable("page", "20230801", pageSchema, parseSqlPage)
	// 	if err != nil {
	// 		errChan <- err
	// 	}
	// }()

	// go func() {
	// 	defer wg.Done()
	// 	err := convertTable("redirect", "20230801", redirectSchema, parseSqlRedirect)
	// 	if err != nil {
	// 		errChan <- err
	// 	}
	// }()

	// go func() {
	// 	wg.Wait()
	// 	close(errChan)
	// }()

	// for err := range errChan {
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }

}
