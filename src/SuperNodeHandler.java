import org.apache.thrift.TException;

public class SuperNodeHandler implements SuperNode.Iface{

    @Override
    public String join(String hostname, int port) throws TException {
        return null;
    }

    @Override
    public String postJoin(String hostname, int port) throws TException {
        return null;
    }

    @Override
    public String getNode() throws TException {
        return null;
    }
}
