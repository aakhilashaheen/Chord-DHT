import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.*;

import java.net.InetAddress;
import java.util.*;

public class NodeHandler1  {

/*


    private Machine self = null;
    private Machine predecessor = null;
    private static FingerTable[] fingerTable;
    int keySpace = 32;
    int maxNodes = 5;
    private Machine successor = null;



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




    //Initialize finger table of local node
    // Coordinates of arbitrary node given to contact
    public boolean initFingerTable(String ipToContact, int portToContact ){
        System.out.println("initFingerTable called");

        try {
            TTransport nodeTransport = new TSocket(ipToContact, portToContact);
            TProtocol nodeProtocol = new TBinaryProtocol(new TFramedTransport(nodeTransport));
            Node.Client nodeClient = new Node.Client(nodeProtocol);
            nodeTransport.open();
            String s = nodeClient.findSuccessor((self.getHashID() + 1) % keySpace);
            System.out.println("Successer found to be " + s);
            fingerTable[0].setSuccessor(new Machine(s));
            successor = new Machine(s);
            nodeTransport.close();
        }catch(TException e){
            System.out.println("Exception while finding successor");
        }
        try{
            TTransport nodeTransport1 = new TSocket(successor.hostname, successor.port);
            TProtocol nodeProtocol1 = new TBinaryProtocol(new TFramedTransport(nodeTransport1));
            Node.Client nodeClient1 = new Node.Client(nodeProtocol1);
            nodeTransport1.open();
            String p = nodeClient1.findPredecessor(successor.getHashID());
            predecessor = new Machine(p);
            //Update the contact nodes predecessor
            nodeClient1.updatePredecessor(self.toString());

            System.out.println("Successor and Predecessor for this new node updated successfully "+ successor.toString() + predecessor.toString());


            nodeTransport1.close();

        } catch (TException e) {
            e.printStackTrace();
        }


        // TODO: May be use nodeClient
        *//* finger [ i + 1] : node = n'.find successor ( finger [ i + 1] : start ) ;  *//*
        for (int i = 0; i < fingerTable.length-1; i++) {

            int fstart = (self.getHashID()+(int)Math.pow(2,i+1))%keySpace;
            System.out.println("Values of fStart" + fstart) ;// start = n+pow(2,i+1) mod pow(2, 32)
            if ( (fstart >= self.getHashID() && fstart<fingerTable[i].getSuccessor().getHashID())
                    || (fstart<fingerTable[i].getSuccessor().getHashID() && fstart >= self.getHashID()) ){
                Machine succ = fingerTable[i].getSuccessor();
                fingerTable[i+1].setSuccessor(succ);
            } else {
                // update with other's successor, which is also the successor
                if(fstart < keySpace && fstart > predecessor.getHashID()){
                    System.out.println("I'm responsible for key " +fstart );
                    fingerTable[i+1].setSuccessor(self);
                }else{
                    try {
                        TTransport transport3 = new TSocket(ipToContact, portToContact);
                        transport3.open();
                        TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport3));
                        Node.Client chordNode = new Node.Client(protocol);
                        System.out.println("Finding successor for :" + fstart);
                        String otherSuccessor = chordNode.findSuccessor(fstart); // first finger : n + pow(2, i) mod pow(2,m)
                        fingerTable[i+1].setSuccessor(new Machine(otherSuccessor));
                        transport3.close();
                    } catch (TException e) {
                        System.err.println("exception at initFingerTable");
                        e.printStackTrace();
                    }
                }

            }
        }
        printFingerTable();


        return true;
    }

    // Update all nodes whose finger tables should refer to n
    public boolean updateOthers(){
        System.out.println("Update others called");
        try {

            for (int i = 0; i < fingerTable.length-1; i++) {
                int id = (self.getHashID() - (int)Math.pow(2, i)) % keySpace;
                System.out.println("Finding predecessor of " + id);
                Machine p = new Machine(findPredecessor(id));
                if(!p.toString().equals(self.toString())){
                    TTransport nodeTransport = new TSocket(p.hostname, p.port);
                    TProtocol nodeProtocol = new TBinaryProtocol(new TFramedTransport(nodeTransport));
                    Node.Client nodeClient = new Node.Client(nodeProtocol);
                    System.out.println("Connecting from " + self.toString() + " to " + p.toString());
                    nodeTransport.open();
                    nodeClient.updateFingerTable(self.toString(), i+1);
                    nodeTransport.close();
                } else {
                    updateFingerTable(self.toString(), i+1);
                }
            }
        } catch (TTransportException e) { System.out.println("TTransportException found of type " + e.getType());
        e.printStackTrace(); }
        catch (Exception e) { e.printStackTrace(); }

        System.out.println("Updated others completed");
        return true;
    }



    public void updateDHT(String nodeInTheSystem) throws TException{

        String ipOfNodeToContact = nodeInTheSystem.split(":")[0];
        int portOfTheNodeToContact = Integer.parseInt(nodeInTheSystem.split(":")[1]);
        System.out.println("Update DHT called");

        if(!ipOfNodeToContact.equals(self.hostname) || portOfTheNodeToContact != self.port ){
            initFingerTable(ipOfNodeToContact, portOfTheNodeToContact);
            //updateOthers();
        }else{
            *//*Case where this is the first node in the system
             *//*
            for(int i = 0; i < fingerTable.length; i++) {
                fingerTable[i].setSuccessor(self);
            }
            predecessor = self;
            successor = self;
        }
    }

    @Override
    public String findSuccessor(int key) throws TException {
        System.out.println("findSuccessor called  for key: "+key);
        if(key == self.getHashID())
            return self.toString();

        Machine keyPredecessor = new Machine(findPredecessor(key));
        if(keyPredecessor.getHashID() == self.getHashID()){
            System.out.println("I'm the node responsible for this key " + key);
            return successor.toString();
        }else{
            System.out.println("In the else loop" +keyPredecessor.toString());
            TTransport nodeTransport = new TSocket(keyPredecessor.hostname, keyPredecessor.port);
            Machine successor = null;
            try {
                //Else forward the request to the closest preceeding finger
                TProtocol nodeProtocol = new TBinaryProtocol(new TFramedTransport(nodeTransport));
                Node.Client nodeClient = new Node.Client(nodeProtocol);
                nodeTransport.open();
                successor = new Machine(nodeClient.findSuccessor(key));
                nodeTransport.close();
            }catch(Exception e){
                System.out.println("Exception in findSuccessor");
                e.printStackTrace();
            }
            return successor == null? null : successor.toString() ;
        }



    }

    @Override
    public String findPredecessor(int key) throws TException {
        Machine other = predecessor;
        if(self.getHashID()==key) return other.toString();

        while(!keyIsInMyIntervalCheck(key, other)) {
            TTransport transport = new TSocket(other.hostname, other.port);
            Machine closestFinger = null;
            System.out.println("Loop " + other.toString()+ key);
            try {
                transport.open();
                TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                Node.Client chordNode = new Node.Client(protocol);
                closestFinger = new Machine(chordNode.closestPrecedingFinger(key));
                transport.close();
            } catch (TTransportException e) {
                System.err.println("exception at findPredecessor");
                e.printStackTrace();
            }

            if(closestFinger == null) break;
            else other = closestFinger;
        }
        return other.toString();
    }

    @Override
    public String closestPrecedingFinger(int key) throws org.apache.thrift.TException {
        for (int i = fingerTable.length-1; i >= 0; --i){
            if(checkFingerRange(fingerTable[i].getSuccessor(), key)) {
                System.out.println("Closest finger found to be" +fingerTable[i].getSuccessor().toString());
                return fingerTable[i].getSuccessor().toString();
            }
        }
        return null;
    }

    private boolean checkFingerRange(Machine fingerI, int key) {
        if (key > self.getHashID()) {
            return (fingerI.getHashID() > self.getHashID()  && fingerI.getHashID() < key);
        } else {
            return (fingerI.getHashID() > self.getHashID() || fingerI.getHashID() < key);
        }
    }



    @Override
    public boolean updatePredecessor(String node) throws TException {
        System.out.println("Uodated predecessor to " + node);
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



    private boolean keyIsInMyIntervalCheck(int key, Machine nPrime){


        Machine primeSuccessor = null;

        // get nPrime's successor Id
        try {
            TTransport nodeTransport = new TSocket(nPrime.hostname, nPrime.port);
            TProtocol nodeProtocol = new TBinaryProtocol(new TFramedTransport(nodeTransport));
            Node.Client nodeClient = new Node.Client(nodeProtocol);
            System.out.println("Trying to open connection to "+ nPrime.toString());
            nodeTransport.open();
            primeSuccessor= new Machine(nodeClient.findSuccessor(nPrime.getHashID()));
            nodeTransport.close();
        } catch (Exception e) {
            System.err.println("exception at findPredecessor");
            e.printStackTrace();
        }

        if(null==primeSuccessor) {
            System.err.println("Dunno what the duck happened!");
            System.exit(1);
        }

        if (primeSuccessor.getHashID() > nPrime.getHashID()) {
            return (key > nPrime.getHashID() && key <= primeSuccessor.getHashID());
        } else {
            return (key > nPrime.getHashID() || key <= primeSuccessor.getHashID());
        }
    }


    @Override
    public boolean updateFingerTable(String node, int index) throws TException {
        System.out.println("Update finger table called for node : " + node + "for index :" + index);
        Machine that = new Machine(node);
        boolean check = that.getHashID() == self.getHashID();
        System.out.println("First check works, evaluates to " + check);
        //check = check || openIntervalCheck(that.getHashID(), self.getHashID(),
        //        fingerTable[index].getSuccessor().getHashID());
        int p = that.getHashID();
        int lower = self.getHashID();
        System.out.println(fingerTable[index] == null);
        int upper = fingerTable[index].getSuccessor().getHashID();
        System.out.println("First argument evaluates to " + p + ", second argument evaluates to " + lower +
               ", third argument evaluates to " + upper);
        if (that.getHashID() == self.getHashID() ||
                openIntervalCheck(that.getHashID(), self.getHashID(), fingerTable[index].getSuccessor().getHashID())) {
            System.out.println("We're in the if statement!");
            System.out.println("Setting finger " + index);
            fingerTable[index].setSuccessor(that);
            //Machine p = new Machine(findPredecessor(self.getHashID() - (int) Math.pow(2, i)));
            System.out.println("Connecting to " + predecessor.toString());
            try {
                TTransport nodeTransport = new TSocket(predecessor.hostname, predecessor.port);
                TProtocol nodeProtocol = new TBinaryProtocol(new TFramedTransport(nodeTransport));
                Node.Client nodeClient = new Node.Client(nodeProtocol);
                nodeTransport.open();
                System.out.println("Connected to " + predecessor.toString());
                nodeClient.updateFingerTable(that.toString(), index);
                nodeTransport.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else { System.out.println("Not doing anything"); }
        printFingerTable();
        return true;
    }

    @Override
    public String getPredecessor() throws TException {
        return predecessor.toString();
    }

    @Override
    public String getSuccessor() throws TException {
        System.out.println("Called getSuccessor with result " + fingerTable[1].getSuccessor().toString());
        return fingerTable[1].getSuccessor().toString();
    }

    *//**
     * Method to get a sorted set of IDs in the system
     *//*
    private TreeSet<Machine> getMachinesSortedByIds(String[] nodes) {
        TreeSet<Machine> sortedMachines = new TreeSet<>();
        for (int i = 0; i<nodes.length; i++){
            String[] splits = nodes[i].split(":");
            sortedMachines.add(new Machine(splits[0], Integer.parseInt(splits[1]), Integer.parseInt(splits[2])));
        }
        return sortedMachines;
    }


    public NodeHandler1(String superNodeIP, Integer superNodePort, Integer port) throws Exception {


        // connect to the supernode as a client
        TTransport superNodeTransport = new TSocket(superNodeIP, superNodePort);

        TProtocol superNodeProtocol = new TBinaryProtocol(new TFramedTransport(superNodeTransport));
        SuperNode.Client superNode = new SuperNode.Client(superNodeProtocol);
        superNodeTransport.open();
        System.out.println("Node has Connected to the SuperNode.");

        //Create a Machine data type representing ourselves
        self = new Machine(InetAddress.getLocalHost().getHostName(), port);

        // call join on superNode for a list
        System.out.println("Calling join");
        String predecessorNode = superNode.join(self.hostname, self.port);
        System.out.println("obtained response" + predecessorNode);
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
        fingerTable = new FingerTable[maxNodes];
            superNode.join(InetAddress.getLocalHost().getHostName(), port);
        for(int i = 0 ; i < fingerTable.length; i++){
            fingerTable[i] = new FingerTable();
            fingerTable[i].setStart((self.getHashID() + (int)Math.pow(2,i)) % keySpace);
        }

        for (int i = 0; i < fingerTable.length-1; i++) {
            fingerTable[i].setInterval(fingerTable[i].getStart(),fingerTable[i+1].getStart());
        }

        fingerTable[maxNodes-1].setInterval(fingerTable[maxNodes-1].getStart(),fingerTable[0].getStart());


        //Update current fingerTable
        updateDHT(info[1]);

        printFingerTable();

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
        TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverTransport);
        serverArgs.processor(processor); //Set handler
        serverArgs.transportFactory(factory); //Set FramedTransport (for performance)

        //Run server as single thread
        TServer server = new TThreadPoolServer(serverArgs);
        server.serve();

    }

    private void printFingerTable() {
        System.out.println("Printing DHT for this node ;");
        for (int i = 0; i < fingerTable.length; i++) {
            System.out.println(i + ":" + fingerTable[i].getSuccessor().toString());

        }
    }*/

}
