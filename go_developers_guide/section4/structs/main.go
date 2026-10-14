package main

import "fmt"

type contactInfo struct {
	email   string
	zipCode int
}

type person struct {
	firstName string
	lastName  string
	contact   contactInfo
}

func main() {
	jim := person{
		firstName: "Jimothy",
		lastName:  "Cricket",
		contact: contactInfo{
			email:   "jimothy@gmail.com",
			zipCode: 12345,
		},
	}

	jim.print()
	jim.updateName(("just jim"))
	jim.print()
}

func (p *person) updateName(newFirstName string) {
	p.firstName = newFirstName
}
func (p person) print() {
	fmt.Printf("%+v", p)
}
