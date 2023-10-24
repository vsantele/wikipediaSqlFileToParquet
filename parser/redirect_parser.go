package parser

import (
	"log"
	"regexp"
	"strconv"

	"github.com/vsantele/wikipediaSqlFileToParquet/common/model"
	"github.com/vsantele/wikipediaSqlFileToParquet/common/utils"
)

func ParseSqlRedirect(line string) []model.Redirect {
	regex := regexp.MustCompile(`\((?P<from>\d+),(?P<namespace>\d+),'(?P<title>(?:\\.|[^\\'])*)',(?P<interwiki>'((?:\\.|[^\\'])*)'|NULL),'((?:\\.|[^\\'])*)'\)`)
	fromIndex := regex.SubexpIndex("from")
	namespaceIndex := regex.SubexpIndex("namespace")
	titleIndex := regex.SubexpIndex("title")
	interwikiIndex := regex.SubexpIndex("interwiki")
	matches := regex.FindAllStringSubmatch(line, -1)
	redirects := make([]model.Redirect, 0, len(matches))
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
		redirect := model.Redirect{
			From:      int64(from),
			Namespace: int64(namespace),
			Title:     utils.Unquote(match[titleIndex]),
			Interwiki: utils.Unquote(match[interwikiIndex]),
		}
		redirects = append(redirects, redirect)
	}
	return redirects
}
