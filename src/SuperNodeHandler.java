import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class SuperNodeHandler implements SuperNode.Iface {

    private boolean joinInProgress = false;
    private int maxNodes = 5;
    private List<Machine> activeNodes = new ArrayList<>();
    private List<Integer> assignedIds = new ArrayList<>();
    private HashService hashService = new HashService(maxNodes);
    int numberOfNodesInTheSystem = 0;
    int[] indexes = new int[]{16,3,11,4,23};

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
            uniqueHashId = uniqueHashId % 8;
        }
        /*Set nodehashes explicitly

         */
//
//        uniqueHashId = indexes[numberOfNodesInTheSystem];
//        numberOfNodesInTheSystem++;

        m.setHashID(uniqueHashId);
        activeNodes.add(m);
        assignedIds.add(uniqueHashId);

        Collections.sort(assignedIds);

        int predID = uniqueHashId;
        Iterator<Integer> iterator = assignedIds.iterator();
        while (iterator.hasNext()) {
            int next = iterator.next();
            if (next < predID) {
                predID = next;
                break;
            }
        }
        if (predID == uniqueHashId)
            predID = Collections.max(assignedIds);

        Machine prev = getNodePrev(predID);
        if(prev == null)
            return "FALSE";
        nodeList += m.toString() + ",";
        System.out.println(nodeList);
        return  uniqueHashId+ "#" + prev.toString();

    }

    @Override
    public String postJoin(String hostname, int port) throws TException {

        Collections.sort(assignedIds,Collections.reverseOrder());
        for(int i = 0 ;i < assignedIds.size() ; i ++){


        }
        joinInProgress = false;

        return "Success";
    }

    @Override
    public String getNode() throws TException {
        System.out.println("Inside getNode");
        // TODO: Make this random
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
