import org.apache.thrift.TException;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class SuperNodeHandler implements SuperNode.Iface {

    private boolean joinInProgress = false;
    private int maxNodes = 3;
    private List<Machine> activeNodes = new ArrayList<>();
    private Set<Integer> assignedIds = new HashSet<>();
    private HashService hashService = new HashService(maxNodes);
    String nodeList = "";



    @Override
    public String join(String hostname, int port) throws TException {

        if(joinInProgress)
            return "NACK";
        else
            joinInProgress = true;

        System.out.println("Received request from a node to join the DHT" + hostname + port);
        Machine m = new Machine(hostname, port);
        int uniqueHashId = hashService.hash(m.toString());
        while(assignedIds.contains(uniqueHashId)){
            System.out.println("This uniqueHashId has already been used" +uniqueHashId);
            uniqueHashId = uniqueHashId + 1;
            uniqueHashId = uniqueHashId % maxNodes;
        }
        m.setHashID(uniqueHashId);
        activeNodes.add(m);
        assignedIds.add(uniqueHashId);
        nodeList += m.toString() + ",";
        System.out.println(nodeList);
        return  uniqueHashId+ "|" + nodeList ;

    }

    @Override
    public String postJoin(String hostname, int port) throws TException {
        joinInProgress = false;
        return "Success";
    }

    @Override
    public String getNode() throws TException {
        // TODO: Make this random
        int index = (int)(Math.random() * (activeNodes.size()));
        return activeNodes.get(index).toString();
    }
}
