import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.*;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class NodeHandler implements Node.Iface {
    Machine self = null;
    Machine predecessor = null;
    Map<Integer, String> fingerTable = new HashMap();

    @Override
    public String setGenre(String bookTitle, String bookGenre) throws TException {
        return null;
    }

    @Override
    public String getGenre(String bookTitle) throws TException {
        return null;
    }

    /* This is used to update the finger table and predecessors of the current node.
     */
    @Override
    public void updateDHT(String nodesInTheSystem) throws TException {
        String [] machines = nodesInTheSystem.split(",");
        TreeSet<Machine> sortedNodesInTheSystem = getMachinesSortedByIds(machines);
        findPredecessor(sortedNodesInTheSystem);

        //Start building the finger table
//        int numberOfEntries = Math.
    }


    private void findPredecessor(TreeSet<Machine> sortedNodesInTheSystem){
        Machine[] allNodes = sortedNodesInTheSystem.toArray(new Machine[sortedNodesInTheSystem.size()]);
        for(int i = 0 ; i< allNodes.length ; i++){
            if(allNodes[i].hostname.equals(self.hostname)){
                if(i == 0){
                    predecessor =  allNodes[allNodes.length-1];
                }else{
                    predecessor = allNodes[i-1];
                }
            }
        }

    }

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
        Machine self = new Machine(InetAddress.getLocalHost().getHostName(), port);


        // call join on superNode for a list
        String nodeInformationReceived = superNode.join(self.hostname, self.port);
        self.setHashID(Integer.parseInt(nodeInformationReceived.split("|")[0]));
        //keep trying until we can join (RPC calls)
        while(nodeInformationReceived.equals("NACK") ){
            System.err.println(" Could not join, retrying ..");
            Thread.sleep(1000);
            nodeInformationReceived = superNode.join(self.hostname, self.port);
        }

        //Extract current node information from the nodeInformationReceived from the super Node
        System.out.println(nodeInformationReceived);

        // populate our own DHT and recursively update others
        self.setHashID(Integer.valueOf(nodeInformationReceived.split(":")[0]));
        updateDHT(nodeInformationReceived.split(":")[1]);

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

}
