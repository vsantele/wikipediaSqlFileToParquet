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
		go func(table string) {
			switch table {
			case "page":
				errCh <- convertTable(root, language, table, date, new(model.Page), parser.ParseSqlPage)
			case "pagelinks":
				errCh <- convertTable(root, language, table, date, new(model.PageLink), parser.ParseSqlPageLinks)
			case "redirect":
				errCh <- convertTable(root, language, table, date, new(model.Redirect), parser.ParseSqlRedirect)
			}
			defer wg.Done()
		}(table)

	}
	wg.Wait()
	close(errCh)
	if len(errCh) > 0 {
		for err := range errCh {
			if err != nil {
				log.Fatal(err)
			}
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
	go write(ch, filenameOut, schema, &wg, name)

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
	log.Println("[" + name + "]\t Write Finished")
	return nil
}

func write[T interface{}](ch chan [](T), fileName string, schema *T, wg *sync.WaitGroup, name string) {
	defer wg.Done()
	log.Println("[" + name + "]\t Write Started")
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
	totalRow := 0
	for buf := range ch {
		iter++
		if len(buf) == 0 {
			continue
		}
		nb, err := writer.Write(buf)
		totalRow += nb
		if err != nil {
			panic(err)
		}
		if (iter % 30) == 0 {
			log.Println("["+name+"]\t Wrote", totalRow, "rows")
			totalRow = 0
		}
	}
	log.Println("["+name+"]\t Wrote", totalRow, "rows. End of writing")
}
