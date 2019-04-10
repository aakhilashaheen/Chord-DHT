import java.net.InetAddress;

public class NodeServer {
    public static void main(String[] args) {
        if(args.length < 4) {
            System.err.println("Usage: java NodeHandler1 <superNodeIP> <superNodePort> <port> <maxNodes>");
            return;
        }
        try {
            System.out.println("Our IP Address is " + InetAddress.getLocalHost().toString());
            String superNodeIP = args[0];
            Integer superNodePort = Integer.parseInt(args[1]);
            //port number used by this node.
            Integer port = Integer.parseInt(args[2]);
            Integer maxNodes = Integer.parseInt(args[3]);
            NodeHandler node = new NodeHandler(superNodeIP, superNodePort, port,maxNodes);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }
}
