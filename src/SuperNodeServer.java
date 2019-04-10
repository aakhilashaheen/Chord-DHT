import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
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
            TServerTransport serverTransport = new TServerSocket(1729);
            //TTransportFactory factory = new TFramedTransport.Factory();

            //Create service request handler
            SuperNodeHandler handler = new SuperNodeHandler();
            SuperNode.Processor processor = new SuperNode.Processor(handler);

            //Run server as a single thread
            TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport)
                    .processor(processor)//.transportFactory(factory)
            );
            server.serve();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}
