package parser

import (
	"regexp"
	"strconv"

	"github.com/vsantele/wikipediaSqlFileToParquet/common/model"
	"github.com/vsantele/wikipediaSqlFileToParquet/common/utils"
)

func ParseSqlPageLinks(line string) []model.PageLink {
	regex := regexp.MustCompile(`\((?P<from>\d+),(?P<namespace>\d+),'(?P<title>(?:\\.|[^\\'])*)',(?P<fromNamespace>\d+)\)`)
	fromIdIndex := regex.SubexpIndex("from")
	toNamespaceIndex := regex.SubexpIndex("namespace")
	toTitleIndex := regex.SubexpIndex("title")
	fromNamespaceIndex := regex.SubexpIndex("fromNamespace")

	matches := regex.FindAllStringSubmatch(line, -1)
	pageLinks := make([]model.PageLink, 0, len(matches))
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

		pageLink := model.PageLink{
			FromId:        int64(fromId),
			FromNamespace: int64(fromNamespace),
			ToTitle:       utils.Unquote(match[toTitleIndex]),
			ToNamespace:   int64(toNamespace),
		}
		pageLinks = append(pageLinks, pageLink)
	}
	return pageLinks
}
