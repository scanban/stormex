package examples.tx;

public class TxMeta {
    private int start;
    private int count;

    public TxMeta(int start, int count) {
        this.start = start;
        this.count = count;
    }

    public int getStart() {
        return start;
    }

    public int getCount() {
        return count;
    }
}
