import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/*The coordinator node that acts as an entry point to every one trying to contact DHT. Stores minimal information
about the nodes present.
 */
public class SuperNodeHandler implements SuperNode.Iface {

    private boolean joinInProgress = false;
    private int keySpace ;
    private List<Machine> activeNodes = new ArrayList<>();
    private List<Integer> assignedIds = new ArrayList<>();
    private HashService hashService;
    private int port;
    private int maxNodes;
    private int currentNumberOfNodesIntheSystem = 0;

    String nodeList = "";


    public SuperNodeHandler(int port, int maxNodes){
        this.maxNodes = maxNodes;
        this.port = port;
        this.hashService = new HashService(maxNodes);
        int minFingerTableSizeNeeded = (int) Math.ceil(Math.log(maxNodes) / Math.log(2));
        this.keySpace = (int) Math.pow(2,minFingerTableSizeNeeded);
        System.out.println("Keyspaces size : "+keySpace);
    }


    @Override
    public String join(String hostname, int port) throws TException {

        if(!joinInProgress) {
            synchronized (this){
                joinInProgress = true;
            }
            if(activeNodes.size() == maxNodes){
                System.out.println("Maximum number of nodes reached in the DHT");
                return "NACK";

            }
            System.out.println("Received join request from : " + hostname + port);
            Machine m = new Machine(hostname, port);
            int keyForNode = hashService.hash(m.toString());
            while(assignedIds.contains(keyForNode)){
                System.out.println("This key has already been used" + keyForNode);
                keyForNode = keyForNode + 1;
                keyForNode = keyForNode % keySpace;
            }
            m.setHashID(keyForNode);
            assignedIds.add(keyForNode);
            activeNodes.add(m);
            Collections.sort(assignedIds, Collections.reverseOrder());

            int predecessorId = keyForNode;
            Iterator<Integer> iterator = assignedIds.iterator();
            while (iterator.hasNext()) {
                int next = iterator.next();
                if (next < predecessorId) {
                    predecessorId = next;
                    break;
                }
            }
            if (predecessorId == keyForNode)
                predecessorId = Collections.max(assignedIds);

            Machine prev = getNodePrev(predecessorId);
            if(prev == null)
                return "FALSE";
            nodeList += m.toString() + " , ";
            System.out.println(nodeList);
            return  keyForNode+ "#" + prev.toString();

        }else{
            return "NACK";
        }

    }

    @Override
    public String postJoin(String machine) throws TException {

        synchronized (this) {
            joinInProgress = false;
            currentNumberOfNodesIntheSystem = activeNodes.size();
            System.out.println("Current nodes in the system" +currentNumberOfNodesIntheSystem);
        }

        return "Success";
    }

    @Override
    public String getNode() throws TException {
        if(currentNumberOfNodesIntheSystem != maxNodes){
            System.out.println("DHT hasn't formed yet!" + currentNumberOfNodesIntheSystem + maxNodes);
            return null;
        }

        System.out.println("Inside getNode");

        String nodes = "";
        for(Machine m : activeNodes) {
            nodes += m.toString() + "#";
        }
        return nodes;
    }

    public Machine getNodePrev(int id){
        for(int i = 0; i < activeNodes.size() ; i++){
            if(activeNodes.get(i).getHashID() == id)
                return activeNodes.get(i);
        }

        return null;
    }
}
