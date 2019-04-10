import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

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
    }


    @Override
    public String join(String hostname, int port) throws TException {

        if(!joinInProgress) {
            synchronized (this){
                joinInProgress = true;
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
            Collections.sort(assignedIds);

            int predID = keyForNode;
            Iterator<Integer> iterator = assignedIds.iterator();
            while (iterator.hasNext()) {
                int next = iterator.next();
                if (next < predID) {
                    predID = next;
                    break;
                }
            }
            if (predID == keyForNode)
                predID = Collections.max(assignedIds);

            Machine prev = getNodePrev(predID);
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
        int index = (int)(Math.random() * (activeNodes.size()));
        return activeNodes.get(index).toString();
    }

    public Machine getNodePrev(int id){
        for(int i = 0; i < activeNodes.size() ; i++){
            if(activeNodes.get(i).getHashID() == id)
                return activeNodes.get(i);
        }

        return null;
    }
}
