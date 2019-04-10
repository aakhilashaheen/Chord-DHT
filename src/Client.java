import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.File;
import java.util.Scanner;

public class Client {

    TTransport serverTransport, nodeTransport;
    SuperNode.Client superNode;
    static Node.Client node;

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

            Machine nodeAddress = client.connectToServer();;
            while(nodeAddress == null) {
                nodeAddress = client.connectToServer();
                System.err.println("Client: Failed to connect to the DHT, retrying in 1 second ...");
                Thread.sleep(1000);
            }

            System.out.println("Contacted node at " + nodeAddress.hostname + ":" + nodeAddress.port);
            System.out.println("\n\n -------- Welcome to the Terminal for book look up --------\n\n");

            Scanner inp = new Scanner(System.in);

            while(true) {
                System.out.print("Enter command > ");
                String command = inp.nextLine();
                if(command.toLowerCase().equals("get")) {
                    System.out.print("Enter book title > ");
                    String bookTitle = inp.nextLine();
                    System.out.println("The genre of " + bookTitle + " is " + node.getGenre(bookTitle));
                } else if(command.toLowerCase().equals("set")) {
                    System.out.print("Enter book title > ");
                    String bookTitle = inp.nextLine();
                    System.out.print("Enter book genre > ");
                    String bookGenre = inp.nextLine();
                    node.setGenre(bookTitle, bookGenre);
                    System.out.println("Title set.");
                } else if(command.toLowerCase().equals("finger")){
                    node.printFingerTable();
                } else {
                    System.out.println("Could not understand command. Please try again.");
                }
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

}
