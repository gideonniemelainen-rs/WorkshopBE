package main

func main() {
	cards := newDeckFromFile("my_cards.txt")
	// Added file extension to save file explicitly as .txt
	cards.shuffle()
	cards.print()
}
