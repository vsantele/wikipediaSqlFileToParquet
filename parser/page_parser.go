package parser

import (
	"regexp"
	"strconv"

	"github.com/vsantele/wikipediaSqlFileToParquet/common/model"
	"github.com/vsantele/wikipediaSqlFileToParquet/common/utils"
)

func ParseSqlPage(line string) []model.Page {
	regex := regexp.MustCompile(`(?i)\((?P<id>\d+),(?P<namespace>\d+),'(?P<title>(?:\\.|[^\\'])*)',(?P<isRedirect>\d),(?P<isNew>\d),([\d\.]+),'((?:\\.|[^\\'])*)',('((?:\\.|[^\\'])*)'|NULL),(?P<latest>\d+),(\d+),('((?:\\.|[^\\'])*)'|NULL),('((?:\\.|[^\\'])*)'|NULL)\)`)
	idIndex := regex.SubexpIndex("id")
	namespaceIndex := regex.SubexpIndex("namespace")
	titleIndex := regex.SubexpIndex("title")
	isRedirectIndex := regex.SubexpIndex("isRedirect")
	isNewIndex := regex.SubexpIndex("isNew")
	latestIndex := regex.SubexpIndex("latest")

	matches := regex.FindAllStringSubmatch(line, -1)
	pages := make([]model.Page, 0, len(matches))
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

		page := model.Page{
			Id:         int64(id),
			Namespace:  int64(namespace),
			Title:      utils.Unquote(match[titleIndex]),
			IsRedirect: match[isRedirectIndex] == "1",
			IsNew:      match[isNewIndex] == "1",
			Latest:     int64(latest),
		}
		pages = append(pages, page)
	}
	return pages
}
