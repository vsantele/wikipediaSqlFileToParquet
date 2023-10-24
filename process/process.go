package process

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"path"

	"github.com/parquet-go/parquet-go"
	"github.com/vsantele/wikipediaSqlFileToParquet/common/model"
	"github.com/vsantele/wikipediaSqlFileToParquet/common/utils"
	"github.com/vsantele/wikipediaSqlFileToParquet/parser"
)

func Process(root string, language string, date string, tables []string) {
	var wg = sync.WaitGroup{}

	errCh := make(chan error, len(tables))

	for _, table := range tables {
		wg.Add(1)
		table := table
		go func(err chan error) {
			switch table {
			case "page":
				err <- convertTable(root, language, table, date, new(model.Page), parser.ParseSqlPage)
				break
			case "pagelinks":
				err <- convertTable(root, language, table, date, new(model.PageLink), parser.ParseSqlPageLinks)
				break
			case "redirect":
				err <- convertTable(root, language, table, date, new(model.Redirect), parser.ParseSqlRedirect)
			}
			wg.Done()
		}(errCh)

	}
	wg.Wait()
	if len(errCh) > 0 {
		for err := range errCh {
			log.Fatal(err)
		}
	}

}

func convertTable[T interface{}](root string, language string, name string, date string, schema *T, parser func(line string) [](T)) error {
	filenameIn := path.Join(root, utils.FilenameConcat(language, date, name, "sql.gz"))
	filenameOut := path.Join(root, utils.FilenameConcat(language, date, name, "parquet"))

	fileInfo, err := os.Stat(filenameIn)
	if os.IsNotExist(err) {
		return fmt.Errorf("file %s does not exist", filenameIn)
	}
	if err != nil {
		return err
	}
	if fileInfo.IsDir() {
		return fmt.Errorf("%s is a directory", filenameIn)
	}

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

	bufferedReader := bufio.NewScanner(reader)
	maxCapacity := 10 * 1024 * 1024
	buf := make([]byte, maxCapacity)
	bufferedReader.Buffer(buf, maxCapacity)

	// Create a new Parquet file writer
	// The file will be written to the specified io.Writer
	// with the given Arrow schema and Parquet configuration
	ch := make(chan [](T), 1000000)
	var wg sync.WaitGroup
	wg.Add(1)
	go write(ch, filenameOut, schema, &wg)

	// string that contains all the insert statements
	for bufferedReader.Scan() {
		line := bufferedReader.Text()

		if strings.HasPrefix(line, headerLine) && strings.HasSuffix(line, ";") {
			buf := parser(line)
			ch <- buf
		}
	}
	if err := bufferedReader.Err(); err != nil {
		log.Fatal(err)
	}
	close(ch)
	wg.Wait()
	log.Println("Write Finished")
	return nil
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
					log.Println("Channel closed, exiting")
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
					log.Println("Wrote", nb, "rows")
				}
			}

		}
	}
}
