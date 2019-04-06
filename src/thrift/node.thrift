service Node {
    string setGenre(1: string bookTitle, 2: string bookGenre),
    string getGenre(1: string bookTitle),
    void updateDHT(1: string nodesInTheSystem),
    string findSuccessor(1: i32 key),
    string findPredecessor(1: i32 key),
    bool updatePredecessor(1: string node),
    bool updateFingerTable(1: string node, 2: i32 index),
    string getPredecessor(),
    string getSuccessor(),
    string closestPrecedingFinger(1: i32 key)
}
