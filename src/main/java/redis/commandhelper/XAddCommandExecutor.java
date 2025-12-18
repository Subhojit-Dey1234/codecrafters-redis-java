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

        // Auto-generate ID if needed
        String finalId = autoGenerateId(key, id);

        // Validate the ID
        String validationError = validateId(key, finalId);
        if (validationError != null) {
            return validationError;
        }

        List<Map<String, String>> lst = this.xaddHashMap.computeIfAbsent(key,
                _ -> Collections.synchronizedList(new ArrayList<>())
        );
        Map<String, String> mp = new ConcurrentHashMap<>();
        mp.put("id", finalId);
        for(int i = 3; i < commands.length-1; i+= 2){
            mp.put(commands[i], commands[i+1]);
        }
        lst.add(mp);
        return "$" + finalId.length() + "\r\n" + finalId + "\r\n";
    }

    private String autoGenerateId(String key, String id) {
        // If ID doesn't contain *, return as-is
        if (!id.contains("*")) {
            return id;
        }

        // Parse the ID
        String[] parts = id.split("-");
        long millis = Long.parseLong(parts[0]);

        // Check if sequence number needs to be auto-generated
        if (parts[1].equals("*")) {
            long sequenceNumber = generateSequenceNumber(key, millis);
            return millis + "-" + sequenceNumber;
        }

        return id;
    }

    private long generateSequenceNumber(String key, long millis) {
        List<Map<String, String>> lst = this.xaddHashMap.get(key);

        // If stream is empty or doesn't exist
        if (lst == null || lst.isEmpty()) {
            // Special case: if time part is 0, start sequence at 1
            return (millis == 0) ? 1 : 0;
        }

        // Find the last entry with the same time part
        long lastSequenceWithSameTime = -1;
        for (int i = lst.size() - 1; i >= 0; i--) {
            Map<String, String> entry = lst.get(i);
            String entryId = entry.get("id");
            String[] entryParts = entryId.split("-");
            long entryMillis = Long.parseLong(entryParts[0]);

            if (entryMillis == millis) {
                lastSequenceWithSameTime = Long.parseLong(entryParts[1]);
                break;
            } else if (entryMillis < millis) {
                // We've gone past entries with this time part
                break;
            }
        }

        // If no entries exist with this time part
        if (lastSequenceWithSameTime == -1) {
            // Special case: if time part is 0, start sequence at 1
            return (millis == 0) ? 1 : 0;
        }

        // Increment the last sequence number
        return lastSequenceWithSameTime + 1;
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
            Map<String, String> lastEntry = lst.getLast();
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

        return null;
    }
}