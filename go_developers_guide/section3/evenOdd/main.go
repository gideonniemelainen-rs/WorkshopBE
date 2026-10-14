package main

import "fmt"

func main() {
	numbers := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	for _, num := range numbers {
		isEven := numberIsEven(num)
		if isEven {
			fmt.Println(num, " is even")
		} else {
			fmt.Println(num, " is odd")
		}
	}
}

func numberIsEven(num int) bool {
	return num%2 == 0
}
