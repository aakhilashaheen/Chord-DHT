public class FingerTable {
    private int start;
    private int intervalStart;
    private int intervalEnd;
    private Machine successor;

    public FingerTable(int start, int intervalStart, int intervalEnd, Machine successor) {
        this.start = start;
        this.intervalStart = intervalStart;
        this.intervalEnd = intervalEnd;
        this.successor = successor;
    }


    public FingerTable() {
    }

    public int getStart() {
        return start;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public int getIntervalStart() {
        return intervalStart;
    }

    public void setIntervalStart(int intervalStart) {
        this.intervalStart = intervalStart;
    }

    public int getIntervalEnd() {
        return intervalEnd;
    }

    public void setIntervalEnd(int intervalEnd) {
        this.intervalEnd = intervalEnd;
    }

    public Machine getSuccessor() {
        return successor;
    }

    public void setSuccessor(Machine successor) {
        this.successor = successor;
    }

    public void setInterval(int start, int end){
        this.intervalStart = start;
        this.intervalEnd = end;
    }
}
