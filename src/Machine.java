public class Machine implements Comparable<Machine>{
    public final String hostname;
    public final int port;
    private int hashID;



    public Machine(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
       // this.hashID = hash(hostname + Integer.toString(port));
    }

    public Machine(String hostname, int port, int hashID){
        this.hostname = hostname;
        this.port = port;
        this.hashID = hashID;

    }

    public Machine (String description){
        this.hostname = description.split(":")[0];
        this.port = Integer.parseInt(description.split(":")[1]);
        this.hashID = Integer.parseInt(description.split(":")[2]);
    }

    public int getHashID() {
        return hashID;
    }

    public void setHashID(int hashID) {
        this.hashID = hashID;
    }

    @Override
    public String toString(){
        return hostname + ":" + port + ":" + hashID;
    }

    /**
     * Method to compare two Machines based on their hashIds
     */
    @Override
    public int compareTo(Machine other) {
        // TODO Auto-generated method stub
        if (this.hashID < other.hashID)
            return -1;
        else if (this.hashID > other.hashID)
            return 1;
        return 0;
    }


}
