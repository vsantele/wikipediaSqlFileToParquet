package model

type Page struct {
	Id         int64
	Namespace  int64
	Title      string
	IsRedirect bool
	IsNew      bool
	Latest     int64
}
