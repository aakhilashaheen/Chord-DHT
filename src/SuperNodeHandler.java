import org.apache.thrift.TException;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class SuperNodeHandler implements SuperNode.Iface {

    private AtomicBoolean joinInProgress ;
    private int maxNodes;
    private List<Machine> activeNodes = new ArrayList<>();
    private Set<Integer> assignedIds = new HashSet<>();
    private HashService hashService;
    String nodeList = "";

    /* Need to pass the maximum number of nodes in the system to setup
     */
    public SuperNodeHandler(int maxNodes) {
        this.maxNodes = maxNodes;
        joinInProgress.set(false);
        hashService = new HashService(maxNodes);
    }

    @Override
    public String join(String hostname, int port) throws TException {
        synchronized (joinInProgress) {
            if(joinInProgress.get())
                return "NACK";
            else
                joinInProgress.set(true);
        }

        Machine m = new Machine(hostname, port);
        int uniqueHashId = hashService.hash(m.toString());
        while(assignedIds.contains(uniqueHashId)){
            uniqueHashId = uniqueHashId++ % maxNodes;
        }
        m.setHashID(uniqueHashId);
        activeNodes.add(m);
        assignedIds.add(uniqueHashId);

        nodeList += m.toString() + ",";
        return  uniqueHashId + nodeList ;

    }

    @Override
    public String postJoin(String hostname, int port) throws TException {
        return "";
    }

    @Override
    public String getNode() throws TException {
        // TODO: Make this random
        int index = (int)(Math.random() * (activeNodes.size()));
        return activeNodes.get(index).toString();
    }
}