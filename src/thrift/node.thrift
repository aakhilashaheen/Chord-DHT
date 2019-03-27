service Node {
    string set(1: string bookTitle, 2: string bookGenre),
    string get(1: string bookTitle),
    void updateDHT()
}
