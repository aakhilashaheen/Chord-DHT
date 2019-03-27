import org.apache.thrift.TException
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class SuperNodeHandler implements SuperNode.Iface {

    private AtomicBoolean joinInProgress;
    private int maxNodes;
    private HashMap<Integer, Machine> activeNodes;

    @Override
    public String join(String hostname, int port) throws TException {
        synchronized (joinInProgress) {
            if(joinInProgress.get())
                return "NACK";
            else
                joinInProgress.set(true);
        }

        Machine m = new Machine(hostname, port);
        String nodeAddress;
        while(activeNodes.containsKey(m.hashID))
            m.hashID++;
        activeNodes.put(m.hashID, m);

        if(activeNodes.size() == 1)
            nodeAddress = m.toString();
        else {
            nodeAddress = getNode();
        }
        return m.hashID + "," + nodeAddress;

    }

    @Override
    public String postJoin(String hostname, int port) throws TException {
        return null;
    }

    @Override
    public String getNode() throws TException {
        // TODO: Make this random
        Integer j = new Integer(0);
        while(!activeNodes.containsKey(j))
            ++j;
        return activeNodes.get(j).toString();
    }
}