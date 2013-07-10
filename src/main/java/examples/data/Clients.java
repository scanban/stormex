package examples.data;

import java.util.HashMap;
import java.util.Map;

public class Clients {
    private static final Map<String, Integer> clients = new HashMap<>();

    static {
        clients.put("78119990001", 1);
        clients.put("78119990002", 2);
        clients.put("78119990003", 3);
        clients.put("78119990004", 4);
        clients.put("78119990005", 5);
        clients.put("78119990006", 6);
        clients.put("78119990007", 7);
    }

    public static Map<String, Integer> getClients() {
        return clients;
    }

    public static int getClientId(String number) {
        Integer rvalue = clients.get(number);
        return rvalue == null ? -1 : rvalue;
    }
}
