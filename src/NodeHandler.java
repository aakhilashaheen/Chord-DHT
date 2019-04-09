import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.*;


import java.net.InetAddress;

public class NodeHandler implements Node.Iface {

    private Machine self = null;
    private Machine predecessor = null;
    private int nodeKey = 0;
    int keySpace = 32;
    int maxNodes = 5;
    private FingerTable[] fingers;

    @Override
    public String setGenre(String bookTitle, String bookGenre) throws TException {
        return null;
    }

    @Override
    public String getGenre(String bookTitle) throws TException {
        return null;
    }

    @Override
    public void updateDHT(String nodesInTheSystem) throws TException {

    }

    @Override
    public String findSuccessor(int key) throws TException {
        System.out.println("Got a query for successor of " + key);
        if (key == self.getHashID())
            return self.toString();
        //Find the predecessor of the key and forward the successor of that node
        Machine n1 = new Machine(findPredecessor(key));
        if (n1.getHashID() == nodeKey)
            return fingers[0].getSuccessor().toString();
        String succ = null;
        try {
            TTransport nodeTransport = new TSocket(n1.hostname, n1.port);
            TProtocol nodeProtocol = new TBinaryProtocol(nodeTransport);
            Node.Client startNodeClient = new Node.Client(nodeProtocol);
            nodeTransport.open();
            System.out.println("Node has Connected to the predecessor node." + n1.toString());
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

    @Override
    public String findPredecessor(int key) throws TException {
        System.out.println("Got a query for findPredecessor " + key);
        Machine other = predecessor;
        int isNormalInterval = 1;
        int succId = fingers[0].getSuccessor().getHashID();
        int myId = nodeKey;
        if (myId >= succId) {
            isNormalInterval = 0;
        }

        if(other.getHashID() == nodeKey)
            return other.toString();

        System.out.println("isNormalInterval value" + isNormalInterval);

        while ((isNormalInterval == 1 && (key <= myId || key > succId))
                || (isNormalInterval == 0 && (key <= myId && key > succId))) {

            if (other.getHashID() != nodeKey) {
                other = new Machine(closestPrecedingFinger(key));
            }

            myId = other.getHashID();

            //Create a connection to the other closest node

            try {
                TTransport nodeTransport = new TSocket(other.hostname, other.port);
                TProtocol nodeProtocol = new TBinaryProtocol(nodeTransport);
                Node.Client startNodeClient = new Node.Client(nodeProtocol);
                System.out.println("Node is trying to Connect to the closestfinger node." + other.toString());
                nodeTransport.open();
                succId = new Machine(startNodeClient.getSuccessor()).getHashID();
                nodeTransport.close();
            } catch (Exception e) {
                System.out.println("Could not find the successor of closestFinger" + other.toString());
                e.printStackTrace();
            }
            if (myId >= succId) {
                isNormalInterval = 0;
            } else {
                isNormalInterval = 1;
            }

        }
        return other.toString();

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
        Machine pred;
        int normalInterval = 1;
        int myId = nodeKey;
        int nextId = fingers[0].getSuccessor().getHashID();
        if (myId >= nextId)
            normalInterval = 0;

        Machine s = new Machine(node);

        if (((normalInterval == 1 && (s.getHashID() >= myId && s.getHashID() < nextId))
                || (normalInterval == 0 && (s.getHashID() >= myId || s.getHashID() < nextId))) && (nodeKey != s.getHashID())) {
            fingers[index].setSuccessor(s);

//            if (predecessor.getHashID() == s.getHashID()){
//                printFingerTable();
//                return true;
//            }
            try {
                if (predecessor.getHashID() != s.getHashID()) {
                    TTransport nodeTransport = new TSocket(predecessor.hostname, predecessor.port);
                    TProtocol nodeProtocol = new TBinaryProtocol(nodeTransport);
                    Node.Client updateFingerTableClient = new Node.Client(nodeProtocol);
                    System.out.println("Node is trying to connect to the predecessor node for finger table update." + predecessor.toString());
                    nodeTransport.open();
                    updateFingerTableClient.updateFingerTable(s.toString(), index);
                    nodeTransport.close();
                }
            } catch (Exception e) {
                System.out.println("Could not update the finger table");
                e.printStackTrace();
            }


        }
        printFingerTable();
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
        int normalInterval = 1;
        if (nodeKey >= key)
            normalInterval = 0;

        for (int i = fingers.length - 1; i >= 0; i--) {
            int succInfo = fingers[i].getSuccessor().getHashID();
            if (normalInterval == 1) {
                if (succInfo > nodeKey && succInfo < key) {
                    System.out.println("The closest preceeding finger is found to be :" + fingers[i].getSuccessor().toString());
                    return fingers[i].getSuccessor().toString();
                }
            } else {
                if (succInfo > nodeKey || succInfo < key) {
                    System.out.println("The closest preceeding finger is found to be :" + fingers[i].getSuccessor().toString());
                    return fingers[i].getSuccessor().toString();
                }
            }
        }
        return self.toString();
    }
    public NodeHandler(String superNodeIP, Integer superNodePort, Integer port) throws Exception {

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
        fingers = new FingerTable[maxNodes];
        //Initialize fingerTable
        initFingerTable(predecessor);
        updateOthers();
        printFingerTable();
        // call post join after all DHTs are updated.
        if (!superNode.postJoin(self.hostname, self.port).equals("Success"))
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
        TServer server = new TSimpleServer(new TServer.Args(serverTransport).processor(processor));
        server.serve();
    }

    private boolean initFingerTable(Machine startNode) {
        System.out.println("initFingerTable called using startNode : " + startNode);
        //Populate all the fingerTable with default values
        for (int i = 0; i < maxNodes; i++) {
            int intervalStart = (nodeKey + (int) Math.pow(2, i)) % keySpace;
            fingers[i] = new FingerTable();
            fingers[i].setIntervalStart(intervalStart);
        }
        for (int i = 0; i < maxNodes - 1; i++) {
            fingers[i].setIntervalEnd(fingers[i + 1].getIntervalStart());
        }
        fingers[maxNodes - 1].setIntervalEnd(fingers[0].getIntervalStart());

        //Case for this being the first node in the ring
        if (startNode.getHashID() == nodeKey) {
            //Set all entries in finger table as self
            for (int i = 0; i < maxNodes; i++) {
                fingers[i].setSuccessor(self);
            }
        } else {
            for (int i = 0; i < maxNodes; i++) {
                fingers[i].setSuccessor(self);
            }
            predecessor = startNode;
            // connect to the startNode as a client
            try {
                TTransport nodeTransport = new TSocket(startNode.hostname, startNode.port);
                TProtocol nodeProtocol = new TBinaryProtocol(nodeTransport);
                Node.Client startNodeClient = new Node.Client(nodeProtocol);
                nodeTransport.open();

                System.out.println("Node has Connected to the predecessor node.");
                int startInterval = fingers[0].getIntervalStart();
                String succ = startNodeClient.findSuccessor(startInterval);
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
                boolean success = successorClient.updateSuccessor(self.toString());
                updatePredecessor(succPred);
                //Also set successors predecessor to self
                successorClient.updatePredecessor(self.toString());
                nodeTransport1.close();
            } catch (Exception e) {
                System.out.println("Error while updating the predecessor of the successor");
                e.printStackTrace();
            }
            int normalInterval = 1;
            //Find successors of all the other entries in the fingerTable
            for (int i = 0; i < maxNodes - 1; i++) {
                System.out.println("Filling finger table entry for : " + fingers[i].getIntervalStart());
                int nextId = fingers[i].getSuccessor().getHashID();
                if (nodeKey >= nextId)
                    normalInterval = 0;

                if ((normalInterval == 1 && (fingers[i + 1].getIntervalStart() >= nodeKey && fingers[i + 1].getIntervalStart() <= nextId))
                        || (normalInterval == 0 && (fingers[i + 1].getIntervalStart() >= nodeKey || fingers[i + 1].getIntervalStart() <= nextId))) {
                    fingers[i + 1].setSuccessor(fingers[i].getSuccessor());
                } else {
                    System.out.println("No optimisation found, searching for successors");
                    try {
                        TTransport nodeTransport2 = new TSocket(startNode.hostname, startNode.port);
                        TProtocol nodeProtocol2 = new TBinaryProtocol(nodeTransport2);
                        Node.Client clientForFingerTable = new Node.Client(nodeProtocol2);
                        nodeTransport2.open();
                        System.out.println("Succesfully connected to the node :" + startNode.toString());
                        String succ = clientForFingerTable.findSuccessor(fingers[i + 1].getIntervalStart());
                        if (succ == null) {
                            System.out.println("No successors found for " + fingers[i + 1].getIntervalStart() + "!!!!!");
                            break;
                        } else {
                            System.out.println("Successor for " + fingers[i + 1].getIntervalStart() + " found to be :" + succ);
                            fingers[i + 1].setSuccessor(new Machine(succ));
                        }
                        nodeTransport2.close();
                    } catch (Exception e) {
                        System.out.println("Error in updating the finger table for entry " + fingers[i].getIntervalStart());
                    }
                }

            }
        }
        printFingerTable();
        return true;
    }


    private void printFingerTable() {
        for (int i = 0; i < maxNodes; i++) {
            System.out.println(fingers[i].getIntervalStart() + "|" + fingers[i].getIntervalEnd() + "|" + fingers[i].getSuccessor().toString());
        }
    }


    private boolean updateOthers() {
        System.out.println("Update others called");
        Machine pred = null;
        for (int i = 0; i < fingers.length; i++) {
            int pkey = nodeKey - (1 << i);
            if (pkey < 0) pkey = pkey + keySpace;
            try {
                pred = new Machine(findPredecessor(pkey));
                if (pred.getHashID() != nodeKey) {
                    TTransport nodeTransport = new TSocket(pred.hostname, pred.port);
                    TProtocol nodeProtocol = new TBinaryProtocol(nodeTransport);
                    Node.Client clientForUpdatingOthers = new Node.Client(nodeProtocol);
                    nodeTransport.open();
                    System.out.println("Succesfully connected to the node :" + pred.toString());
                    clientForUpdatingOthers.updateFingerTable(self.toString(), i);
                    nodeTransport.close();
                }

            } catch (Exception e) {
                System.out.println("Error in updating the finger table for entry " + i + " in node " + pred.toString());
                e.printStackTrace();
            }


        }
        return true;
    }
}
