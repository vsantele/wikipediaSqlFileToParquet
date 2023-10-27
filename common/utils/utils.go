package utils

import "strings"

func Unquote(input string) string {
	if input == "''" {
		return ""
	}
	return strings.Replace(strings.Replace(strings.Replace(input, `\'`, `'`, -1), `\"`, `"`, -1), "\\\\", "\\", -1)

}

func FilenameConcat(language string, date string, name string, extension string) string {
	return language + "wiki-" + date + "-" + name + "." + extension
}
