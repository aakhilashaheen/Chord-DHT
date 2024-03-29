import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.*;


import java.net.InetAddress;
import java.util.*;


/* This is the main node server. This provides methods to join the Chord ring, setting up the DHT and serving Client requests
 */
public class NodeHandler implements Node.Iface {

    private Machine self = null;
    private Machine predecessor = null;
    private int nodeKey = 0;
    int fingerTableSize;
    int keySpace;
    private FingerTable[] fingers;
    private static Map<String, String> bookGenreMap= new HashMap<String, String>();
    private static HashService hashService;


    /* Used by the client to set the genre of books in the system

     */
    @Override
    public String setGenre(String bookTitle, String bookGenre) throws TException{
        System.out.println("Trying to set the genre at this node : " + self.toString());
        int bookkey = HashService.hash(bookTitle);
        System.out.println("Hash of the book is evaluated as " + bookkey);
        String result = "";
        if(responsibilityCheck(predecessor.getHashID(), self.getHashID(), bookkey)){
            System.out.println("Node responsible for insert : "+ self.toString());
            bookGenreMap.put(bookTitle, bookGenre);
        }else{
            Machine destNode = new Machine(closestPrecedingFinger(HashService.hash(bookTitle)));
            if(destNode.getHashID() == self.getHashID()){
                try {
                    TTransport nodeTransport = new TSocket(fingers[0].getSuccessor().hostname, fingers[0].getSuccessor().port);
                    TProtocol nodeProtocol = new TBinaryProtocol(nodeTransport);
                    Node.Client successorClient = new Node.Client(nodeProtocol);
                    System.out.println("Node is trying to connect to the destination node." + destNode.toString());
                    nodeTransport.open();
                    result = successorClient.setGenre(bookTitle, bookGenre);
                    nodeTransport.close();
                }catch(Exception e){
                    e.printStackTrace();
                }
            }else {
                System.out.println("ClosestPreceeding node found to be !!!!!!" + destNode.toString());
                try {
                    TTransport nodeTransport = new TSocket(destNode.hostname, destNode.port);
                    TProtocol nodeProtocol = new TBinaryProtocol(nodeTransport);
                    Node.Client closestPreceedingClient = new Node.Client(nodeProtocol);
                    System.out.println("Node is trying to connect to the destination node." + destNode.toString());
                    nodeTransport.open();
                    result = closestPreceedingClient.setGenre(bookTitle, bookGenre);
                    nodeTransport.close();
                } catch (Exception e) {
                    System.out.println("Could not insert book genre into new node");
                    e.printStackTrace();
                }
            }
        }

        return result + "##" + self.toString();
    }


    /*Used by client to get the genre of the books in the system

     */
    @Override
    public String getGenre(String bookTitle) throws TException{
        System.out.println("Trying to get the genre at this node : " + self.toString());
        int bookkey = HashService.hash(bookTitle);
        System.out.println("Hash of the book is evaluated as " + bookkey);
        String result = "";

        if(responsibilityCheck(predecessor.getHashID(), self.getHashID(), bookkey)){
            System.out.println("Key for the book is" +HashService.hash(bookTitle));
            System.out.println("Node responsible for insert : "+ self.toString());
            if(!bookGenreMap.containsKey(bookTitle))
                result = "BOOK_NOT_FOUND";
            else
                result = bookGenreMap.get(bookTitle);
        }else{
            Machine destNode = new Machine(closestPrecedingFinger(HashService.hash(bookTitle)));
            if(destNode.getHashID() == self.getHashID()) {
                try {
                    TTransport nodeTransport = new TSocket(fingers[0].getSuccessor().hostname, fingers[0].getSuccessor().port);
                    TProtocol nodeProtocol = new TBinaryProtocol(nodeTransport);
                    Node.Client successorClient = new Node.Client(nodeProtocol);
                    System.out.println("Node is trying to connect to the destination node." + destNode.toString());
                    nodeTransport.open();
                    result = successorClient.getGenre(bookTitle);
                    nodeTransport.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }else{
                try {
                    TTransport nodeTransport = new TSocket(destNode.hostname, destNode.port);
                    TProtocol nodeProtocol = new TBinaryProtocol(nodeTransport);
                    Node.Client closestPreceedingClient = new Node.Client(nodeProtocol);
                    System.out.println("Node is trying to connect to the destination node." + destNode.toString());
                    nodeTransport.open();
                    result = closestPreceedingClient.getGenre(bookTitle);
                    nodeTransport.close();
                } catch (Exception e) {
                    System.out.println("Could not insert book genre into new node");
                    e.printStackTrace();
                }
            }
        }

        return result + "##" + self.toString();
    }

/*Used to build the DHT when a node joins
 */
    @Override
    public String findSuccessor(int key) throws TException {
        System.out.println("Got a query for successor of " + key);

        if(intervalCheck(self.getHashID(), fingers[0].getIntervalStart(),key)){
            return self.toString();
        }
        //Find the predecessor of the key and forward the successor of that node
        Machine n1 = new Machine(findPredecessor(key));
        if (n1.getHashID() == nodeKey)
            return fingers[0].getSuccessor().toString();
        String succ = null;
        try {
            TTransport nodeTransport = new TSocket(n1.hostname, n1.port);
            TProtocol nodeProtocol = new TBinaryProtocol(nodeTransport);
            Node.Client startNodeClient = new Node.Client(nodeProtocol);
            System.out.println("Node is trying to connect to the predecessor node." + n1.toString()+ "for findSucc() of key: "+key);
            nodeTransport.open();

            succ = startNodeClient.getSuccessor();
            nodeTransport.close();
        } catch (Exception e) {
            System.out.println("Could not update the successor of new node");
            e.printStackTrace();
        }

        if (succ == null) {
            System.out.println("Could not find succ using findSuccessor !!!!!!");
        }
        System.out.println("Successor found to be " + succ);
        return succ;
    }

    /*Finds the predecessor of the input key

     */
    @Override
    public String findPredecessor(int key) throws TException {
        System.out.println("Got a query for findPredecessor " + key);
        Machine otherNode = self;
        int clockwiseDirection = 1;
        int succId = fingers[0].getSuccessor().getHashID();
        int currentId = nodeKey;
        if (currentId >= succId) {
            clockwiseDirection = 0;
        }

        while ((clockwiseDirection == 1 && (key <= currentId || key > succId))
                || (clockwiseDirection == 0 && (key <= currentId && key > succId))) {

            //Create a connection to the other closest node
            if(otherNode.getHashID() == nodeKey)
                otherNode = new Machine(closestPrecedingFinger(key));
            else{
                try {
                    TTransport nodeTransport = new TSocket(otherNode.hostname, otherNode.port);
                    TProtocol nodeProtocol = new TBinaryProtocol(nodeTransport);
                    Node.Client startNodeClient = new Node.Client(nodeProtocol);
                    System.out.println("Node is trying to Connect to the closestfinger node." + otherNode.toString());
                    nodeTransport.open();
                    otherNode = new Machine(startNodeClient.closestPrecedingFinger(key));
                    currentId = otherNode.getHashID();
                }catch (Exception e) {
                    System.out.println("Could not find the successor of closestFinger" + otherNode.toString());
                    e.printStackTrace();
                }
            }

            if(otherNode.getHashID() == nodeKey){
                succId = fingers[0].getSuccessor().getHashID();
            }else{
                try {
                    TTransport nodeTransport2 = new TSocket(otherNode.hostname, otherNode.port);
                    TProtocol nodeProtocol2 = new TBinaryProtocol(nodeTransport2);
                    Node.Client closestPreceedingFingerclient= new Node.Client(nodeProtocol2);
                    System.out.println("Node is trying to connect to the closest preceeding node to find the succ." + otherNode.toString());
                    nodeTransport2.open();
                    succId = new Machine(closestPreceedingFingerclient.getSuccessor()).getHashID();
                    nodeTransport2.close();

                } catch (Exception e) {
                    System.out.println("Could not connect to the closest preceeding finger" + otherNode.toString());
                    e.printStackTrace();
                }

            }
            if (currentId >= succId) {
                clockwiseDirection = 0;
            } else {
                clockwiseDirection = 1;
            }
           }



        return otherNode.toString();

    }

    private boolean intervalCheck(int start, int end, int key){
        if(start < end){
            return (key >= start && key <end);
        }else{
            return (key >= start || key < end );
        }
    }


    private boolean responsibilityCheck(int predecessorId, int selfId, int key) {
        if(predecessorId > selfId) {
            return key > predecessorId || key <= selfId;
        }else{
            return key > predecessorId && key <= selfId;
        }
    }

    @Override
    public boolean updatePredecessor(String node) throws TException {
        System.out.println("Updating the predecessor to : " + node);
        predecessor = new Machine(node);
        return true;
    }

    @Override
    public boolean updateSuccessor(String node) throws TException {
        System.out.println("Updating the successor to : " + node);
        fingers[0].setSuccessor(new Machine(node));
        return true;
    }

    @Override
    public boolean updateFingerTable(String node, int index) throws TException {
        System.out.println("UpdateFingerTable called for node:" + node + " and index: " + index);
        int clockWiseDirection = 1;
        int currentId = nodeKey;
        int nextId = fingers[index].getSuccessor().getHashID();
        if (currentId >= nextId)
            clockWiseDirection = 0;

        Machine current = new Machine(node);

        if (((clockWiseDirection == 1 && (current.getHashID() >= currentId && current.getHashID() < nextId))
                || (clockWiseDirection == 0 && (current.getHashID() >= currentId || current.getHashID() < nextId))) && (nodeKey != current.getHashID())) {
            fingers[index].setSuccessor(current);
            printFingerTable();

            try {
                if (predecessor.getHashID() != current.getHashID()) {
                    TTransport nodeTransport = new TSocket(predecessor.hostname, predecessor.port);
                    TProtocol nodeProtocol = new TBinaryProtocol(nodeTransport);
                    Node.Client updateFingerTableClient = new Node.Client(nodeProtocol);
                    System.out.println("Node is trying to connect to the predecessor node for finger table update." + predecessor.toString());
                    nodeTransport.open();
                    updateFingerTableClient.updateFingerTable(current.toString(), index);
                    nodeTransport.close();

                }
            } catch (Exception e) {
                System.out.println("Could not update the finger table");
                e.printStackTrace();
            }


        }

        return true;
    }

    @Override
    public String getPredecessor() throws TException {
        return predecessor.toString();
    }

    @Override
    public String getSuccessor() throws TException {
        return fingers[0].getSuccessor().toString();
    }


    @Override
    public String closestPrecedingFinger(int key) throws TException {
        System.out.println("Received request for closestPreceedingFinger for key : " + key);
        int clockWiseDirection = 1;
        int currentId = nodeKey;
        if (currentId >= key)
            clockWiseDirection = 0;

        for (int i = fingers.length - 1; i >= 0; i--) {
            int nodeId = fingers[i].getSuccessor().getHashID();
            if (clockWiseDirection == 1) {
                if (nodeId > currentId && nodeId < key) {
                    System.out.println("The closest preceeding finger is found to be :" + fingers[i].getSuccessor().toString());
                    return fingers[i].getSuccessor().toString();
                }
            } else {
                if (nodeId > currentId || nodeId < key) {
                    System.out.println("The closest preceeding finger is found to be :" + fingers[i].getSuccessor().toString());
                    return fingers[i].getSuccessor().toString();
                }
            }
        }
        return self.toString();
    }
    public NodeHandler(String superNodeIP, Integer superNodePort, Integer port, Integer maxNodes) throws Exception {
        fingerTableSize = (int) Math.ceil(Math.log(maxNodes) / Math.log(2));
        keySpace = (int) Math.pow(2,fingerTableSize);
        hashService = new HashService(maxNodes);
        // connect to the supernode as a client
        TTransport superNodeTransport = new TSocket(superNodeIP, superNodePort);
        TProtocol superNodeProtocol = new TBinaryProtocol(superNodeTransport);
        SuperNode.Client superNode = new SuperNode.Client(superNodeProtocol);
        try {
            superNodeTransport.open();
            System.out.println("Node has Connected to the SuperNode.");
        } catch (TTransportException e) {
            e.printStackTrace();
        }
        String predecessorNode = superNode.join(InetAddress.getLocalHost().getHostName(), port);
        System.out.println("Node has Connected to the SuperNode.");
        while (predecessorNode.equals("NACK")) {
            System.err.println(" Could not join, retrying ..");
            Thread.sleep(1000);
            predecessorNode = superNode.join(self.hostname, self.port);
        }

        System.out.println("Received information from superNode : " + predecessorNode);
        String[] info = predecessorNode.split("#");
        //Create self instance
        self = new Machine(InetAddress.getLocalHost().getHostName(), port, Integer.valueOf(info[0]));
        nodeKey = Integer.valueOf(info[0]);
        predecessor = new Machine(info[1]);
        fingers = new FingerTable[fingerTableSize];


        for (int i = 0; i < fingerTableSize; i++) {
            int intervalStart = (nodeKey + (int) Math.pow(2, i)) % keySpace;
            fingers[i] = new FingerTable();
            fingers[i].setIntervalStart(intervalStart);
        }
        for (int i = 0; i < fingerTableSize - 1; i++) {
            fingers[i].setIntervalEnd(fingers[i + 1].getIntervalStart());
        }
        fingers[fingerTableSize - 1].setIntervalEnd(fingers[0].getIntervalStart());
        //First node in the ring
        if(self.getHashID() == predecessor.getHashID()){
            for(int i = 0 ; i < fingerTableSize ; i ++){
                fingers[i].setSuccessor(self);
            }
        }else{
        //Initialize fingerTable
            initFingerTable(predecessor);
            updateOthers();
            printFingerTable();
        }

        // call post join after all DHTs are updated.
        if (!superNode.postJoin(self.toString()).equals("Success"))
            System.err.println("Machine(" + self.getHashID() + ") Could not perform postJoin call.");

        superNodeTransport.close();
        start();
    }

    //Begin Thrift Server instance for a Node and listen for connections on our port
    private void start() throws TException {
        //Create Thrift server socket
        TServerTransport serverTransport = new TServerSocket(self.port);

        Node.Processor processor = new Node.Processor<>(this);
        //Run server as a single thread
        TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
        server.serve();
    }

    private boolean initFingerTable(Machine startNode) {
        System.out.println("initFingerTable called using startNode : " + startNode);
        //Populate all the fingerTable with default values

            for (int i = 0; i < fingerTableSize; i++) {
                fingers[i].setSuccessor(self);
            }
            predecessor = startNode;
            // connect to the startNode as a client
            TTransport nodeTransport = new TSocket(startNode.hostname, startNode.port);
            try {

                TProtocol nodeProtocol = new TBinaryProtocol(nodeTransport);
                Node.Client startNodeClient = new Node.Client(nodeProtocol);
                nodeTransport.open();

                System.out.println("Node has Connected to the predecessor node.");
                int startInterval = fingers[0].getIntervalStart();
                String succ = startNodeClient.findSuccessor(startInterval);
                System.out.println("Successor found for new node!!!!" + succ);
                fingers[0].setSuccessor(new Machine(succ));
                nodeTransport.close();
            } catch (Exception e) {
                System.out.println("Could not update the successor of new node");
                e.printStackTrace();
            }

            //Create a connection to the successor and find its predecessor
            try {
                TTransport nodeTransport1 = new TSocket(fingers[0].getSuccessor().hostname, fingers[0].getSuccessor().port);
                TProtocol nodeProtocol1 = new TBinaryProtocol(nodeTransport1);
                Node.Client successorClient = new Node.Client(nodeProtocol1);
                nodeTransport1.open();

                System.out.println("Connected to successor node" + fingers[0].getSuccessor().toString());
                String succPred = successorClient.getPredecessor();

                //Also set successors predecessor to self
                boolean success = successorClient.updatePredecessor(self.toString());
                nodeTransport1.close();
                Thread.sleep(10);
            } catch (Exception e) {
                System.out.println("Error while updating the predecessor of the successor");
                e.printStackTrace();
            }
            int clockWiseDirection = 1;
            //Find successors of all the other entries in the fingerTable
            for (int i = 0; i < fingerTableSize - 1; i++) {
                int currentId = nodeKey;
                int nextId = fingers[i].getSuccessor().getHashID();

                if(currentId >= nextId)
                    clockWiseDirection = 0;
                else
                    clockWiseDirection = 1;

                if( (clockWiseDirection ==1 && (fingers[i+1].getIntervalStart() >= currentId && fingers[i+1].getIntervalStart() <= nextId))
                    || (clockWiseDirection == 0 &&  (fingers[i+1].getIntervalStart() >= currentId || fingers[i+1].getIntervalStart() <= nextId)))
                {
                    fingers[i + 1].setSuccessor(fingers[i].getSuccessor());
                } else {
                    System.out.println("No optimisation found, searching for successors");
                    try {
                        TTransport nodeTransport2 = new TSocket(startNode.hostname, startNode.port);
                        TProtocol nodeProtocol2 = new TBinaryProtocol(nodeTransport2);
                        Node.Client clientForFingerTable = new Node.Client(nodeProtocol2);
                        nodeTransport2.open();
                        System.out.println("Successfully connected to the node :" + startNode.toString());
                        String succ = clientForFingerTable.findSuccessor(fingers[i + 1].getIntervalStart());
                        if (succ == null) {
                            System.out.println("No successors found for " + fingers[i + 1].getIntervalStart() + "!!!!!");
                            break;
                        } else {
                            System.out.println("Successor for " + fingers[i + 1].getIntervalStart() + " found to be :" + succ);
                            int intervalStart = fingers[i+1].getIntervalStart();
                            int succHash = new Machine(succ).getHashID();
                            int intervalSuccessorStart = fingers[i+1].getSuccessor().getHashID();
                            if (intervalStart > intervalSuccessorStart)
                                succ = succ + keySpace;
                            if (intervalStart > intervalSuccessorStart)
                                intervalSuccessorStart = intervalSuccessorStart + keySpace;
                            if(intervalStart <= succHash && succHash <= intervalSuccessorStart){
                                fingers[i + 1].setSuccessor(new Machine(succ));
                            }
                        }
                        nodeTransport2.close();
                    } catch (Exception e) {
                        System.out.println("Error in updating the finger table for entry " + fingers[i+1].getIntervalStart());
                        e.printStackTrace();
                    }
                }



            }

        printFingerTable();
        return true;
    }


    public void printFingerTable() {
        for (int i = 0; i < fingerTableSize; i++) {
            System.out.println(fingers[i].getIntervalStart() + "|" + fingers[i].getIntervalEnd() + "|" + fingers[i].getSuccessor().toString());
        }
        System.out.println("Node Info : "+" IP : "+self.hostname + " : Port: " +self.port+ " :key :" + self.getHashID()  );
        System.out.println("Predecessor : "+ predecessor.toString());
        System.out.println("Entries at stored at this node are: ");
        for(Map.Entry bookGenre : bookGenreMap.entrySet()){
            System.out.println(bookGenre.getKey() + ":" +bookGenre.getValue());
        }
    }


    private boolean updateOthers() {
        System.out.println("Update others called");
        Machine pred = null;
        for (int i = 0; i < fingers.length; i++) {
            int id = nodeKey - (int)Math.pow(2,i) + 1;
            if (id < 0)
                id = id + keySpace;
            try {

                pred = new Machine(findPredecessor(id));
                if(pred.getHashID() != nodeKey) {
                    TTransport nodeTransport = new TSocket(pred.hostname, pred.port);
                    TProtocol nodeProtocol = new TBinaryProtocol(nodeTransport);
                    Node.Client clientForUpdatingOthers = new Node.Client(nodeProtocol);
                    nodeTransport.open();
                    System.out.println("Calling updateFingerTable for :" + pred.getHashID() + "for index :" + i);
                    clientForUpdatingOthers.updateFingerTable(self.toString(), i);
                    nodeTransport.close();
                }

            } catch (Exception e) {
                System.out.println("Error in updating the finger table for entry " + i + " in node " + pred.toString());
                e.printStackTrace();
            }

        }
        System.out.println("Printing after update others!!!!");
        return true;
    }
}
