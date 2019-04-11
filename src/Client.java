import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Scanner;

public class Client {

    static TTransport serverTransport, nodeTransport;
    static SuperNode.Client superNode;
    static Node.Client node;
    static String activeNodes;

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
            activeNodes = superNode.getNode();
            Machine nodeAddress = new Machine(activeNodes.split("#")[0]);
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
            System.out.println("\n\n -------- Welcome to the Terminal for book look up --------\n" +
                    "Choose one of get, set, finger or file.\n\n");


            Scanner inp = new Scanner(System.in);

            while(true) {
                try {
                    System.out.print("Enter command > ");
                    String command = inp.nextLine();
                    if (command.toLowerCase().equals("get")) {
                        System.out.print("Enter book title > ");
                        String bookTitle = inp.nextLine();
                        String[] results = node.getGenre(bookTitle).split("##");
                        if (results[0].equals("BOOK_NOT_FOUND"))
                            System.out.println("Book not found.");
                        else
                            System.out.println("The genre of " + bookTitle + " is " + results[0]);
                        boolean first = true;
                        for (int i = 1; i < results.length; ++i)
                            if (!results[i].isEmpty()) {
                                if (first) {
                                    System.out.println("Found at" + results[i]);
                                    first = false;
                                } else
                                    System.out.println("via " + results[i]);
                            }
                    } else if (command.toLowerCase().equals("set")) {
                        System.out.print("Enter book title > ");
                        String bookTitle = inp.nextLine();
                        System.out.print("Enter book genre > ");
                        String bookGenre = inp.nextLine();
                        String[] results = node.setGenre(bookTitle, bookGenre).split("##");
                        System.out.println("Title set.");
                        boolean first = true;
                        for (int i = 0; i < results.length; ++i)
                            if (!results[i].isEmpty()) {
                                if (first) {
                                    System.out.println("Set at" + results[i]);
                                    first = false;
                                } else
                                    System.out.println("via " + results[i]);
                            }
                    } else if (command.toLowerCase().equals("finger")) {
                        node.printFingerTable();
                        String[] nodes = activeNodes.split("#");
                        for (int i = 1; i < nodes.length; ++i) {
                            if (nodes[i].isEmpty())
                                continue;
                            Machine thisNode = new Machine(nodes[i]);
                            TTransport tempTransport = new TSocket(thisNode.hostname, thisNode.port);
                            TProtocol tempProtocol = new TBinaryProtocol(tempTransport);
                            tempTransport.open();
                            Node.Client tempClient = new Node.Client(tempProtocol);
                            tempClient.printFingerTable();
                            tempTransport.close();
                        }

                    } else if (command.toLowerCase().equals("file")) {
                        System.out.print("Enter filename > ");
                        String filename = inp.nextLine();
                        BufferedReader file = new BufferedReader(new FileReader(filename));
                        String line;
                        int success = 0, lines = 0;
                        while ((line = file.readLine()) != null) {
                            String[] book = line.split(":");
                            node.setGenre(book[0], book[1]);
                            String result = node.getGenre(book[0]).split("##")[0];
                            if (book[1].equals(result))
                                ++success;
                            else
                                System.out.println("The book " + book[0] + ", " + book[1] + " could not be set correctly.");
                            ++lines;
                        }
                        file.close();
                        if (success == lines)
                            System.out.println("All " + lines + "/" + success + " books were set correctly.");
                        else
                            System.out.println("Only " + success + "/" + lines + " books were set correctly.");
                    } else {
                        System.out.println("Could not understand command. Please try again.");
                    }
                }catch(Exception ex){
                    System.out.println("Could not complete the request in the DHT due to internal error!");
                }
            }
        }
        catch(Exception e) {
            System.out.println("Exception occured in the DHT!!!!");
            e.printStackTrace();
        }
    }

}
