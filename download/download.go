package download

import (
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"sync"

	"github.com/vsantele/wikipediaSqlFileToParquet/common/utils"
)

const wikimediaDumpUrl = "https://dumps.wikimedia.org/"

func Download(root string, language string, date string, tables []string) {
	var wg = sync.WaitGroup{}
	for _, table := range tables {
		table := table
		wg.Add(1)
		go func() {
			downloadFile(root, language, date, table)
			wg.Done()
		}()
	}
	wg.Wait()
}

func downloadFile(root string, language string, date string, name string) {
	fileName := utils.FilenameConcat(language, date, name, "sql.gz")
	url := wikimediaDumpUrl + language + "wiki/" + date + "/" + fileName
	outputFile := path.Join(root, fileName)

	log.Println("Start Downloading: " + outputFile)
	res, err := http.Get(url)
	if err != nil {
		log.Fatal(err)
	}
	defer res.Body.Close()

	if _, err := os.Stat(outputFile); os.IsExist(err) {
		log.Panicln("The output file already exist")
	}
	file, err := os.Create(outputFile)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	_, err = io.Copy(file, res.Body)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(fileName + " writed")

}
