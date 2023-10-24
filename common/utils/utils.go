package utils

import "strings"

func Unquote(input string) string {
	if input == "''" {
		return ""
	}
	return strings.ReplaceAll(input, "\\'", "'")

}

func FilenameConcat(language string, date string, name string, extension string) string {
	return language + "wiki-" + date + "-" + name +"."+ extension
}
