service SuperNode { 
    string join(1: string hostname, 2: i32 port),
    string postJoin(1: string hostname, 2: i32 port),
    string getNode()
}
