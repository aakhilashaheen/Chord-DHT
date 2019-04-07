import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.*;

import java.net.InetAddress;
import java.util.*;

public class NodeHandler implements Node.Iface {
    Machine self = null;
    Machine predecessor = self;
    private static FingerTable[] fingerTable;
    int keySpace = 32;
    int maxNodes = 5;



    @Override
    public String setGenre(String bookTitle, String bookGenre) throws TException {
        int bookTitleHash = HashService.hash(bookTitle);
        System.out.println("Hash of the book is :" +bookTitleHash);

        return null;
    }

    @Override
    public String getGenre(String bookTitle) throws TException {

        return null;
    }

//    /* This is used to update the finger table and predecessors of the current node.
//     */
//    @Override
//    public void updateDHT(String nodesInTheSystem) throws TException {
//        String [] machines = nodesInTheSystem.split(",");
//        TreeSet<Machine> sortedNodesInTheSystem = getMachinesSortedByIds(machines);
//        findPredecessor(sortedNodesInTheSystem);
//
//        //Start building the finger table
//        int entries = (int) Math.ceil(Math.log(maxNodes) / Math.log(2));
//        for (int i = 0; i<entries; i++){
//            //Find the successor of the identifier (id + 2^(i)) in the system
//            Machine succ = findSuccessor(sortedNodesInTheSystem, (self.getHashID() + (int)Math.pow(2, i)));
//            fingerTable.put(i, succ);/*
//        }
//
//        //Print finger table for confirmation
//        System.out.println("Printing DHT for this node" );
//        for(Map.Entry entry : fingerTable.entrySet()){
//            System.out.println("Key :" + entry.getKey() + "Value : " + entry.getValue());
//        }
//    }


    //Initialize finger table of local node
    // Coordinates of arbitrary node given to contact
    public boolean initFingerTable(String ipToContact, int portToContact ){
        System.out.println("initFingerTable called");
        try {
            TTransport nodeTransport = new TSocket(ipToContact, portToContact);
            TProtocol nodeProtocol = new TBinaryProtocol(new TFramedTransport(nodeTransport));
            Node.Client nodeClient = new Node.Client(nodeProtocol);
            nodeTransport.open();
            String s = nodeClient.findSuccessor(fingerTable[1].getStart());
            fingerTable[1].setSuccessor(new Machine(s));
            nodeTransport.close();

            TTransport nodeTransport1 = new TSocket(fingerTable[1].getSuccessor().hostname, fingerTable[1].getSuccessor().port);
            TProtocol nodeProtocol1 = new TBinaryProtocol(new TFramedTransport(nodeTransport1));
            Node.Client nodeClient1 = new Node.Client(nodeProtocol1);
            nodeTransport1.open();
            String p = nodeClient1.findPredecessor(fingerTable[1].getSuccessor().getHashID());
            predecessor = new Machine(p);
            //Update the contact nodes predecessor
            nodeClient1.updatePredecessor(self.toString());


            // TODO: May be use nodeClient
            /* finger [ i + 1] : node = n'.find successor ( finger [ i + 1] : start ) ;  */
            for (int i = 1; i < fingerTable.length -1; i++) {
                String succ = nodeClient1.findSuccessor(fingerTable[i+1].getStart());


                //TODO: Check successor conditions
                fingerTable[i+1].setSuccessor(new Machine(succ));

            }
            printFingerTable();
            nodeTransport.close();

        } catch (TException e) {
            e.printStackTrace();
        }

        return true;
    }

    // Update all nodes whose finger tables should refer to n
    public boolean updateOthers(){
        System.out.println("Update others called");
        try {

            for (int i = 0; i < fingerTable.length; i++) {
                int id = (self.getHashID() - (int)Math.pow(2,i-1) + 1) % keySpace;
                Machine p = new Machine(findPredecessor(id));
                if(!p.toString().equals(self.toString())){
                    TTransport nodeTransport = new TSocket(p.hostname, p.port);
                    TProtocol nodeProtocol = new TBinaryProtocol(new TFramedTransport(nodeTransport));
                    Node.Client nodeClient = new Node.Client(nodeProtocol);
                    nodeTransport.open();
                    nodeClient.updateFingerTable(self.toString(), i);
                    nodeTransport.close();
                }
            }
        } catch (Exception e) { e.printStackTrace(); }
        return true;
    }



    public void updateDHT(String nodeInTheSystem) throws TException{

        String ipOfNodeToContact = nodeInTheSystem.split(":")[0];
        int portOfTheNodeToContact = Integer.parseInt(nodeInTheSystem.split(":")[1]);
        System.out.println("Update DHT called");

        /*Case where this is the first node in the system
         */
        for(int i = 1; i < fingerTable.length; i++) {
            fingerTable[i].setSuccessor(self);
        }

        if(!ipOfNodeToContact.equals(self.hostname) || portOfTheNodeToContact != self.port ){
            initFingerTable(ipOfNodeToContact, portOfTheNodeToContact);
            updateOthers();
        }
    }

    @Override
    public String findSuccessor(int key) throws TException {
        System.out.println("findSuccessor called");
        //Check to see if all the finger table
        Machine that = new Machine(findPredecessor(key));
        System.out.println("Predecessor found" + that.toString());
        String succ = null;
        if(!that.toString().equals(self.toString())){
            TTransport nodeTransport = new TSocket(that.hostname, that.port);
            TProtocol nodeProtocol = new TBinaryProtocol(new TFramedTransport(nodeTransport));
            Node.Client nodeClient = new Node.Client(nodeProtocol);
            nodeTransport.open();
            succ = nodeClient.getSuccessor();
            nodeTransport.close();
        }else{
            succ = getSuccessor();
        }
        return succ;
    }

    @Override
    public String findPredecessor(int id) throws TException {
        Machine n = self;
        int myID = n.getHashID();
        int succID = fingerTable[1].getSuccessor().getHashID();
        int normalInterval = 1;


        if (myID >= succID)
            normalInterval = 0;
        while ((normalInterval==1 && (id <= myID || id > succID)) ||
                (normalInterval==0 && (id <= myID && id > succID))) {


//            String request = "closetPred/" + id ;
//            String result = makeConnection(n.getIP(),n.getPort(),request);
//            String[] tokens = result.split("/");
//
//            n = new Node(Integer.parseInt(tokens[0]),tokens[1],tokens[2]);

            
            TTransport nodeTransport = new TSocket(n.hostname, n.port);
            TProtocol nodeProtocol = new TBinaryProtocol(new TFramedTransport(nodeTransport));
            Node.Client nodeClient = new Node.Client(nodeProtocol);
            System.out.println("Connecting to server " + n.hostname + "with port: "+n.port );
            nodeTransport.open();
            n = new Machine(nodeClient.closestPrecedingFinger(id));
            nodeTransport.close();



            myID = n.getHashID();

//            String request2 = "getSuc/" ;
//            String result2 = makeConnection(n.getIP(),n.getPort(),request2);
//            String[] tokens2 = result2.split("/");

            TTransport nodeTransport1 = new TSocket(n.hostname, n.port);
            TProtocol nodeProtocol1 = new TBinaryProtocol(new TFramedTransport(nodeTransport1));
            Node.Client nodeClient1 = new Node.Client(nodeProtocol);
            nodeTransport1.open();
            succID = (new Machine(nodeClient1.getSuccessor())).getHashID();
            nodeTransport1.close();

            if (myID >= succID)
                normalInterval = 0;
            else normalInterval = 1;
        }
        //System.out.println("Returning n" + n.getID());

        return n.toString();

//        System.out.println("findPredecessor called " + key);
//       Machine ans = self;
//       Machine succ = fingerTable[1].getSuccessor();
//       while(!(openIntervalCheck(key, ans.getHashID(), succ.getHashID()) || key == succ.getHashID())) {
//           System.out.println("In the findPredecessor loop");
//           if(ans.getHashID() == self.getHashID()){
//               System.out.println("Calling closest preceding finger in me");
//               Machine ans1 = new Machine(closestPrecedingFinger(key));
//               if(ans1.toString().equals(ans.toString()))
//                   break;
//           }else{
//               TTransport nodeTransport = new TSocket(ans.hostname, ans.port);
//               TProtocol nodeProtocol = new TBinaryProtocol(new TFramedTransport(nodeTransport));
//               Node.Client nodeClient = new Node.Client(nodeProtocol);
//               nodeTransport.open();
//               System.out.println("Finding the closest preceeding finger using machine :" + ans.toString());
//               ans = new Machine(nodeClient.closestPrecedingFinger(key));
//               nodeTransport.close();
//           }
//
//           TTransport nodeTransport = new TSocket(ans.hostname, ans.port);
//           TProtocol nodeProtocol = new TBinaryProtocol(new TFramedTransport(nodeTransport));
//           Node.Client nodeClient = new Node.Client(nodeProtocol);
//           nodeTransport.open();
//           succ = new Machine(nodeClient.getSuccessor());
//           nodeTransport.close();
//       }
//        System.out.println("Predecessor for key: "+key +"found to be: "+ans.toString());
//       return ans.toString();
    }

    @Override
    public boolean updatePredecessor(String node) throws TException {
        predecessor = new Machine(node);
        return true;
    }

    private static boolean openIntervalCheck(int p, int lower, int upper) {
        System.out.println("In open interval check with p: "+p+" lower :" + lower + "upper :" + upper);
        if(lower <= upper)
            return lower < p && p < upper;
        else
            return lower < p || p < upper;
    }


    @Override
    public boolean updateFingerTable(String node, int index) throws TException {
        Machine that = new Machine(node);
        if (that.getHashID() == self.getHashID() ||
                openIntervalCheck(that.getHashID(), self.getHashID(), fingerTable[index].getSuccessor().getHashID())) {
            fingerTable[index].setSuccessor(that);
            //Machine p = new Machine(findPredecessor(self.getHashID() - (int) Math.pow(2, i)));
            try {
                TTransport nodeTransport = new TSocket(predecessor.hostname, predecessor.port);
                TProtocol nodeProtocol = new TBinaryProtocol(new TFramedTransport(nodeTransport));
                Node.Client nodeClient = new Node.Client(nodeProtocol);
                nodeTransport.open();
                nodeClient.updateFingerTable(that.toString(), index);
                nodeTransport.close();
            } catch (Exception e) {
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
        return fingerTable[1].getSuccessor().toString();
    }

    @Override
    public String closestPrecedingFinger(int id) throws TException {



        int normalInterval = 1;
        int myID = self.getHashID();
        if (myID >= id) {
            normalInterval = 0;
        }

        for (int i = maxNodes; i >= 1; i--) {
            int nodeID = fingerTable[i].getSuccessor().getHashID();
            if (normalInterval == 1) {
                if (nodeID > myID && nodeID < id)
                    return fingerTable[i].getSuccessor().toString();
            } else {
                if (nodeID > myID || nodeID < id)
                    return fingerTable[i].getSuccessor().toString();
            }
        }
        return self.toString();
//        for(int i = finger.length -1 ; i >= 0 ; i--){
//            if(openIntervalCheck(finger[i].getHashID(), self.getHashID(), key))
//                return finger[i].toString();
//        }
//        return self.toString();
    }

//    /**
//     * Method to find the successor of the identifier 'key' in the system.
//     */
//    private Machine findSuccessor(TreeSet<Machine> sortedNodes, int key) {
//        Iterator<Machine> it = sortedNodes.iterator();
//        key = key % maxNodes ;
//        while (it.hasNext()){
//            Machine element = it.next();
//            if (element.compareTo(new Machine(null, 0, key)) != -1){
//                return element;
//            }
//        }
//        it = sortedNodes.iterator();
//        return it.next();
//    }
//
//
//    private void findPredecessor(TreeSet<Machine> sortedNodesInTheSystem){
//        Machine[] allNodes = sortedNodesInTheSystem.toArray(new Machine[sortedNodesInTheSystem.size()]);
//        for(int i = 0 ; i< allNodes.length ; i++){
//            if(allNodes[i].hostname.equals(self.hostname)){
//                if(i == 0){
//                    predecessor =  allNodes[allNodes.length-1];
//                }else{
//                    predecessor = allNodes[i-1];
//                }
//            }
//        }
//
//    }

    /**
     * Method to get a sorted set of IDs in the system
     */
    private TreeSet<Machine> getMachinesSortedByIds(String[] nodes) {
        TreeSet<Machine> sortedMachines = new TreeSet<>();
        for (int i = 0; i<nodes.length; i++){
            String[] splits = nodes[i].split(":");
            sortedMachines.add(new Machine(splits[0], Integer.parseInt(splits[1]), Integer.parseInt(splits[2])));
        }
        return sortedMachines;
    }


    public NodeHandler(String superNodeIP, Integer superNodePort, Integer port) throws Exception {


        // connect to the supernode as a client
        TTransport superNodeTransport = new TSocket(superNodeIP, superNodePort);

        TProtocol superNodeProtocol = new TBinaryProtocol(new TFramedTransport(superNodeTransport));
        SuperNode.Client superNode = new SuperNode.Client(superNodeProtocol);
        superNodeTransport.open();
        System.out.println("Node has Connected to the SuperNode.");

        //Create a Machine data type representing ourselves
        self = new Machine(InetAddress.getLocalHost().getHostName(), port);

        // call join on superNode for a list
        String predecessorNode = superNode.join(self.hostname, self.port);
        //keep trying until we can join (RPC calls)
        while(predecessorNode.equals("NACK") ){
            System.err.println(" Could not join, retrying ..");
            Thread.sleep(1000);
            predecessorNode = superNode.join(self.hostname, self.port);
        }

        //Extract current node information from the nodeInformationReceived from the super Node
        System.out.println("Predecessor node information received from the superNode : " + predecessorNode);

        String[] info = predecessorNode.split("#") ;
        // populate our own DHT and recursively update others
        self.setHashID(Integer.valueOf(info[0]));
        fingerTable = new FingerTable[maxNodes + 1];

        for(int i = 1 ; i < fingerTable.length; i++){
            fingerTable[i] = new FingerTable();
            fingerTable[i].setStart((self.getHashID() + (int)Math.pow(2,i-1)) % keySpace);
        }

        for (int i = 1; i < fingerTable.length-1; i++) {
            fingerTable[i].setInterval(fingerTable[i].getStart(),fingerTable[i+1].getStart());
        }

        fingerTable[maxNodes].setInterval(fingerTable[maxNodes].getStart(),fingerTable[1].getStart()-1);


        //Update current fingerTable
        updateDHT(info[1]);

        printFingerTable();


//        //Update DHT for all the nodes in the system
//        String[] nodes = allNodeInformation[1].split(",");
//        for (int i=0;i<nodes.length;i++){
//            String[] nodeInfo = nodes[i].split(":");
//            String IP = nodeInfo[0];
//            if(!IP.equals(self.hostname)  || (Integer.parseInt(nodeInfo[1])) != self.port) {
//                int nodeport = Integer.parseInt(nodeInfo[1]);
//                System.out.println("Calling updateDHT for node : "+nodeInfo[0]+" : "+nodeport);
//                TTransport nodeTransport = new TSocket(IP, nodeport);
//                TProtocol nodeProtocol = new TBinaryProtocol(new TFramedTransport(nodeTransport));
//                Node.Client nodeClient = new Node.Client(nodeProtocol);
//                nodeTransport.open();
//                nodeClient.updateDHT(nodeInformationReceived);
//                nodeTransport.close();
//            }
//        }


        // call post join after all DHTs are updated.
        if(!superNode.postJoin(self.hostname, self.port).equals("Success"))
            System.err.println("Machine("+self.getHashID()+") Could not perform postJoin call.");

        superNodeTransport.close();
        start();
    }
    //Begin Thrift Server instance for a Node and listen for connections on our port
    private void start() throws TException {
        //Create Thrift server socket
        TServerTransport serverTransport = new TServerSocket(self.port);
        TTransportFactory factory = new TFramedTransport.Factory();

        Node.Processor processor = new Node.Processor<>(this);

        //Set Server Arguments
        TServer.Args serverArgs = new TServer.Args(serverTransport);
        serverArgs.processor(processor); //Set handler
        serverArgs.transportFactory(factory); //Set FramedTransport (for performance)

        //Run server as single thread
        TServer server = new TSimpleServer(serverArgs);
        server.serve();
    }

    private void printFingerTable() {
        System.out.println("Printing DHT for this node ;");
        for (int i = 1; i < 6; i++) {
            System.out.println(i + ":" + fingerTable[i].getSuccessor().toString());

        }
    }

}
