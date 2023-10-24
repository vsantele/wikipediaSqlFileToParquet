package utils

import "strings"

func Unquote(input string) string {
	if input == "''" {
		return ""
	}
	return strings.ReplaceAll(input, "\\'", "'")

}
