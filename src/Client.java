import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.File;

public class Client {

    TTransport serverTransport, nodeTransport;
    SuperNode.Client superNode;
    Node.Client node;

    public Client(Machine server) {
        serverTransport = new TSocket(server.hostname, server.port);
        System.out.println("Client initialized.");
    }

    private Machine connectToServer() {
        try {
            serverTransport.open();
            System.out.println("Connected to server");
            TProtocol serverProtocol = new TBinaryProtocol(serverTransport);
            superNode = new SuperNode.Client(serverProtocol);
            Machine nodeAddress = new Machine(superNode.getNode());
            System.out.println("Address received" + nodeAddress.toString());
            serverTransport.close();
            System.out.println("Server connection closed.");

            nodeTransport = new TSocket(nodeAddress.hostname, nodeAddress.port);
            TProtocol nodeProtocol = new TBinaryProtocol(nodeTransport);
            nodeTransport.open();
            node = new Node.Client(nodeProtocol);
            return nodeAddress;
        }
        catch(TException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) {
        if(args.length < 2) {
            System.out.println("Client <server-hostname> <server-IP>");
            return;
        }
        try {
            Machine serverInfo = new Machine(args[0], Integer.parseInt(args[1]));
            Client client = new Client(serverInfo);

            Machine nodeAddress;
            do {
                nodeAddress = client.connectToServer();
                System.err.println("Client: Failed to connect to the DHT, retrying in 1 second ...");
                Thread.sleep(1000);
            } while(nodeAddress == null);

            System.out.println("Contacted node at " + nodeAddress.hostname + ":" + nodeAddress.port);
            System.out.println("\n\n -------- Welcome to the Terminal for book look up --------\n\n");


            client.node.setGenre("Alice in Wonderland", "Fantasy");
            String result = client.node.getGenre("Alice in Wonderland");
            System.out.println("Result worked! " + result);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

}
