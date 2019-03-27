service Node {
    string setGenre(1: string bookTitle, 2: string bookGenre),
    string getGenre(1: string bookTitle),
    void updateDHT()
}
