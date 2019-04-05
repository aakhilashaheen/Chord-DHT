public class Machine {
    public final String hostname;
    public final int port;
    private int hashID;

    public Machine(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
       // this.hashID = hash(hostname + Integer.toString(port));
    }

    public int getHashID() {
        return hashID;
    }

    public void setHashID(int hashID) {
        this.hashID = hashID;
    }

    @Override
    public String toString() {
        return "Machine{" +
                "hostname='" + hostname + '\'' +
                ", port=" + port +
                '}';
    }
}
