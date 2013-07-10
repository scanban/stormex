package examples.data;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class Rater {
    private final Map<Integer, Tariff> tariffs = new HashMap<>();

    private static class Tariff  {
        private static final Pattern[] callPatterns = {
                Pattern.compile("0.*"),
                Pattern.compile("1.*"),
                Pattern.compile("2.*"),
                Pattern.compile("3.*"),
                Pattern.compile("4.*"),
                Pattern.compile("5.*"),
                Pattern.compile("6.*"),
                Pattern.compile("7.*"),
                Pattern.compile("8.*"),
                Pattern.compile("9.*")
        };
        private static final int price[] = { 10, 20, 30, 40, 50, 60, 70, 80, 90, 100 };

        private int getPrice(String dest) {
            for(int i = 0; i < callPatterns.length; ++i) {
                if(callPatterns[i].matcher(dest).matches()) { return price[i]; }
            }

            return 0;
        }
    }

    public Rater() {
        for(int c: Clients.getClients().values()) {
            tariffs.put(c, new Tariff());
        }
    }

    public int calculate(int clientId, int callTime, String callDest) {
        return callTime * tariffs.get(clientId).getPrice(callDest);
    }
}
