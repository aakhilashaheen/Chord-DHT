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
    Machine successor = self;
    Map<Integer, Machine> fingerTable = new HashMap();
    int maxNodes = 32;
    Machine [] finger = new Machine[(int) Math.ceil(Math.log(maxNodes) / Math.log(2))];


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
//            fingerTable.put(i, succ);
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
            /*finger[1].node = n.find successor ( finger[1].start ) ;
            predecessor = successor.predecessor;
            successor.predecessor = n ;*/
            String s = nodeClient.findSuccessor((self.getHashID() + 1) % maxNodes);
            finger[0] = new Machine(s);
            successor = finger[0];
            String p = nodeClient.findPredecessor(successor.getHashID());
            predecessor = new Machine(p);
            //Update the contact nodes predecessor
            nodeClient.updatePredecessor(self.toString());
            /* finger [ i + 1] : node = n'.find successor ( finger [ i + 1] : start ) ;  */
            for (int i = 1; i < finger.length; i++) {
                finger[i] = new Machine(nodeClient.findSuccessor(self.getHashID() + (int) Math.pow(2, i)));
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
            for (int i = 0; i < finger.length; i++) {
                Machine p = new Machine(findPredecessor(self.getHashID() - (int) Math.pow(2, i)));
                TTransport nodeTransport = new TSocket(p.hostname, p.port);
                TProtocol nodeProtocol = new TBinaryProtocol(new TFramedTransport(nodeTransport));
                Node.Client nodeClient = new Node.Client(nodeProtocol);
                nodeTransport.open();
                nodeClient.updateFingerTable(self.toString(), i);
                nodeTransport.close();
            }
        } catch (Exception e) { e.printStackTrace(); }
        return true;
    }



    public void updateDHT(String nodeInTheSystem) throws TException{
        String ipOfNodeToContact = nodeInTheSystem.split(":")[0];
        int portOfTheNodeToContact = Integer.parseInt(nodeInTheSystem.split(":")[1]);
        System.out.println("Update DHT called");
        if(!ipOfNodeToContact.equals(self.hostname) || portOfTheNodeToContact != self.port ){

            initFingerTable(ipOfNodeToContact, portOfTheNodeToContact);
            updateOthers();

        }else{
            /*Case where this is the first node in the system
             */
            for(int i = 0; i < finger.length; i++) {
                finger[i] = self;
            }
            predecessor = self;
            successor = self;
        }
    }

    @Override
    public String findSuccessor(int key) throws TException {
        System.out.println("findSuccessor called");
        Machine that = new Machine(findPredecessor(key));
        TTransport nodeTransport = new TSocket(that.hostname, that.port);
        TProtocol nodeProtocol = new TBinaryProtocol(new TFramedTransport(nodeTransport));
        Node.Client nodeClient = new Node.Client(nodeProtocol);
        nodeTransport.open();
        String succ = nodeClient.getSuccessor();
        return succ;
    }

    @Override
    public String findPredecessor(int key) throws TException {
        System.out.println("findPredecessor called");
       Machine ans = self;
       Machine succ = successor;
       while(notIntervalCheck(key, ans.getHashID(), succ.getHashID() )){
           if(ans.getHashID() == self.getHashID()){
               ans = new Machine(closestPrecedingFinger(key));
           }else{
               TTransport nodeTransport = new TSocket(ans.hostname, ans.port);
               TProtocol nodeProtocol = new TBinaryProtocol(new TFramedTransport(nodeTransport));
               Node.Client nodeClient = new Node.Client(nodeProtocol);
               nodeTransport.open();
               ans = new Machine(nodeClient.closestPrecedingFinger(key));
               nodeTransport.close();
           }

           TTransport nodeTransport = new TSocket(ans.hostname, ans.port);
           TProtocol nodeProtocol = new TBinaryProtocol(new TFramedTransport(nodeTransport));
           Node.Client nodeClient = new Node.Client(nodeProtocol);
           nodeTransport.open();
           succ = new Machine(nodeClient.getSuccessor());
           nodeTransport.close();
       }

       return ans.toString();
    }

    @Override
    public boolean updatePredecessor(String node) throws TException {
        predecessor = new Machine(node);
        return true;
    }


    private static boolean notIntervalCheck(int p, int lower, int upper) {
        if(lower <= upper)
            return !(p < lower && p <= upper);
        else
            return !(p < lower || p <= upper);
    }

    private static boolean intervalCheck(int p, int lower, int upper) {
        if(lower <= upper)
            return lower <= p && p < upper;
        else if(p < lower)
            return p < upper;
        else
            return lower <= p;

    }

    private static boolean openIntervalCheck(int p, int lower, int upper) {
        if(lower <= upper)
            return lower < p && p < upper;
        else if(p < lower)
            return p < upper;
        else
            return lower < p;

    }


    @Override
    public boolean updateFingerTable(String node, int index) throws TException {
        Machine that = new Machine(node);
        if (intervalCheck(that.getHashID(), self.getHashID(), finger[index].getHashID())) {
            finger[index] = that;
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
        return true;
    }

    @Override
    public String getPredecessor() throws TException {
        return predecessor.toString();
    }

    @Override
    public String getSuccessor() throws TException {
        return successor.toString();
    }

    @Override
    public String closestPrecedingFinger(int key) throws TException {
        for(int i = finger.length -1 ; i >= 0 ; i--){
            if(openIntervalCheck(finger[i].getHashID(), self.getHashID(), key))
                return finger[i].toString();
        }
        return self.toString();
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
        superNodeTransport.open();
        TProtocol superNodeProtocol = new TBinaryProtocol(new TFramedTransport(superNodeTransport));
        SuperNode.Client superNode = new SuperNode.Client(superNodeProtocol);

        System.out.println("Node has Connected to the SuperNode.");

        //Create a Machine data type representing ourselves
        self = new Machine(InetAddress.getLocalHost().getHostName(), port);

        // call join on superNode for a list
        String nodeInformationReceived = superNode.join(self.hostname, self.port);
        //keep trying until we can join (RPC calls)
        while(nodeInformationReceived.equals("NACK") ){
            System.err.println(" Could not join, retrying ..");
            Thread.sleep(1000);
            nodeInformationReceived = superNode.join(self.hostname, self.port);
        }

        //Extract current node information from the nodeInformationReceived from the super Node
        System.out.println("Node information received from the superNode : " + nodeInformationReceived);

        String[] allNodeInformation = nodeInformationReceived.split("#") ;
        // populate our own DHT and recursively update others
        self.setHashID(Integer.valueOf(allNodeInformation[0]));
//        System.out.println("allNodeInformation[1]"+ allNodeInformation[1]);

        //Update current fingerTable
        updateDHT(allNodeInformation[1]);

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
        for (int i = 0; i < 5; i++) {
            System.out.println(i + ":" + finger[i].hostname + ":" + finger[i].port + ":" + finger[i].getHashID());

        }
    }

}
