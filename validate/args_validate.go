package validate

var tables = []string{"page", "pagelinks", "redirect"}

func IsTableNameValid(name string) bool {
	for _, table := range tables {
		if table == name {
			return true
		}
	}
	return false
}
