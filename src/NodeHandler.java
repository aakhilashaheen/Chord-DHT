import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.*;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;

public class NodeHandler implements Node.Iface {
    Integer nodeID;
    Integer port;
    HashMap<String,String> fileSystem;
    @Override
    public String setGenre(String bookTitle, String bookGenre) throws TException {
        return null;
    }

    @Override
    public String getGenre(String bookTitle) throws TException {
        return null;
    }

    @Override
    public void updateDHT() throws TException {

    }
    public NodeHandler(String superNodeIP, Integer superNodePort, Integer port) throws Exception {

        this.port = port;

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

        //keep trying until we can join (RPC calls)
        while(nodeInformationReceived.equals("NACK") ){
            System.err.println(" Could not join, retrying ..");
            Thread.sleep(1000);
            nodeInformationReceived = superNode.join(self.hostname, self.port);
        }

        //Extract current node information from the nodeInformationReceived from the super Node
        System.out.println(nodeInformationReceived);

        // populate our own DHT and recursively update others
        updateDHT();

        // call post join after all DHTs are updated.
        if(!superNode.postJoin(self.hostname, self.port).equals("Success"))
            System.err.println("Machine("+nodeID+") Could not perform postJoin call.");

        superNodeTransport.close();
        start();
    }
    //Begin Thrift Server instance for a Node and listen for connections on our port
    private void start() throws TException {
        //Create Thrift server socket
        TServerTransport serverTransport = new TServerSocket(this.port);
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
