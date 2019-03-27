public class Machine {
    public final String hostname;
    public final int port;
    public int hashID;

    public Machine(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
        this.hashID = hash(hostname + Integer.toString(port));
    }

    private int hash(String obj) {
        // Implement later
        return 0;
    }

    @Override
    public String toString() {
        return this.hostname + ":" + Integer.toString(this.port) + ":" + Integer.toString(this.hashID);
    }
}
