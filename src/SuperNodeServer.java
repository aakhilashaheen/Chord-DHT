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
            if(arg.length < 2) {
                System.out.println("Expected input {superNodePort} {maxNodesInTheSystem}");
                return;
            }
            //Create Thrift server socket
            TServerTransport serverTransport = new TServerSocket(Integer.parseInt(arg[0]));
            //TTransportFactory factory = new TFramedTransport.Factory();

            //Create service request handler
            SuperNodeHandler handler = new SuperNodeHandler(Integer.parseInt(arg[0]), Integer.parseInt(arg[1]));
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
