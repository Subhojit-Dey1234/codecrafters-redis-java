package redis.commandhelper;

import redis.interfce.IRedisCommandExecutor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class XAddCommandExecutor implements IRedisCommandExecutor {

    private final Map<String, List<Map<String, String>>> xaddHashMap;

    public XAddCommandExecutor(
            Map<String, List<Map<String, String>>> xaddHashMap){
        this.xaddHashMap = xaddHashMap;
    }


    @Override
    public String getMessage(String[] commands) {
        String key = commands[1];
        String id = commands[2];

        // Validate the ID first
        String validationError = validateId(key, id);
        if (validationError != null) {
            return validationError;
        }

        List<Map<String, String>> lst = this.xaddHashMap.computeIfAbsent(key,
                _ -> Collections.synchronizedList(new ArrayList<>())
        );
        Map<String, String> mp = new ConcurrentHashMap<>();
        mp.put("id", id);
        for(int i = 3; i < commands.length-1; i+= 2){
            mp.put(commands[i], commands[i+1]);
        }
        lst.add(mp);
        return "$" + id.length() + "\r\n" + id + "\r\n";
    }

    private String validateId(String key, String id) {
        // Check if ID is 0-0
        if ("0-0".equals(id)) {
            return "-ERR The ID specified in XADD must be greater than 0-0\r\n";
        }

        // Parse the new ID
        String[] parts = id.split("-");
        long newMillis = Long.parseLong(parts[0]);
        long newSeq = Long.parseLong(parts[1]);

        // Check if newMillis is 0 and newSeq is 0 (alternative check)
        if (newMillis == 0 && newSeq == 0) {
            return "-ERR The ID specified in XADD must be greater than 0-0\r\n";
        }

        // Get the list of existing entries
        List<Map<String, String>> lst = this.xaddHashMap.get(key);

        // If stream exists and has entries, validate against last entry
        if (lst != null && !lst.isEmpty()) {
            Map<String, String> lastEntry = lst.get(lst.size() - 1);
            String lastId = lastEntry.get("id");
            String[] lastParts = lastId.split("-");
            long lastMillis = Long.parseLong(lastParts[0]);
            long lastSeq = Long.parseLong(lastParts[1]);

            // Check if new ID is greater than last ID
            if (newMillis < lastMillis ||
                    (newMillis == lastMillis && newSeq <= lastSeq)) {
                return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
            }
        }

        return null; // Validation passed
    }
}