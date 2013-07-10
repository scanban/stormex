package examples.data;

import java.io.Serializable;

public class Cdr implements Serializable {
    private final String callSource;
    private final String callDestination;
    private final int callTime;
    private int clientId;
    private int price;

    public Cdr(String callSource, String callDestination, int callTime) {
        this.callSource = callSource;
        this.callDestination = callDestination;
        this.callTime = callTime;
    }

    public String getCallSource() {
        return callSource;
    }

    public String getCallDestination() {
        return callDestination;
    }

    public int getCallTime() {
        return callTime;
    }

    public int getClientId() {
        return clientId;
    }

    public int getPrice() {
        return price;
    }

    public void setClientId(int clientId) {
        this.clientId = clientId;
    }

    public void setPrice(int price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "Cdr{" +
                "callSource='" + callSource + '\'' +
                ", callDestination='" + callDestination + '\'' +
                ", callTime=" + callTime +
                ", clientId=" + clientId +
                ", price=" + price +
                '}';
    }
}
