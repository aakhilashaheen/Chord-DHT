import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportFactory;

public class SuperNodeServer {
    public static void main(String arg[]){
        try{
            if(arg.length < 1) {
                System.out.println("Expected the maximum number of nodes in the system");
                return;
            }
            //Create Thrift server socket
            TServerTransport serverTransport = new TServerSocket(Integer.parseInt(arg[0]));
            TTransportFactory factory = new TFramedTransport.Factory();

            //Create service request handler
            SuperNodeHandler handler = new SuperNodeHandler(Integer.getInteger(arg[0]));
            SuperNode.Processor processor = new SuperNode.Processor(handler);

            //Set server arguments
            TServer.Args args = new TServer.Args(serverTransport);
            args.processor(processor);	 //Set handler
            args.transportFactory(factory);  //Set FramedTransport (for performance)

            //Run server as a single thread
            TServer server = new TSimpleServer(args);
            server.serve();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}
