package redis.commandhelper;

import redis.interfce.IRedisCommandExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class XReadCommandExecutor implements IRedisCommandExecutor {

    private final Map<String, List<Map<String, String>>> xaddHashMap;

    public XReadCommandExecutor(Map<String, List<Map<String, String>>> xaddHashMap){
        this.xaddHashMap = xaddHashMap;
    }

    @Override
    public String getMessage(String[] commands) {
        String key = commands[2];
        String startId = commands[3];

        List<Map<String, String>> listStream = xaddHashMap.get(key);

        // If stream doesn't exist or is empty, return null bulk string
        if (listStream == null || listStream.isEmpty()) {
            return "$-1\r\n";
        }

        // Find all entries with ID greater than startId (exclusive)
        List<Map<String, String>> entriesAfterStart = new ArrayList<>();
        for (Map<String, String> entry : listStream) {
            String entryId = entry.get("id");
            if (compareIds(entryId, startId) > 0) {
                entriesAfterStart.add(entry);
            }
        }

        // If no entries found after startId, return null bulk string
        if (entriesAfterStart.isEmpty()) {
            return "$-1\r\n";
        }

        // Build RESP response
        StringBuilder response = new StringBuilder();

        // Outer array: array of streams (we only have 1 stream)
        response.append("*1\r\n");

        // Each stream is an array of 2 elements: [key, entries]
        response.append("*2\r\n");

        // First element: stream key as bulk string
        response.append("$").append(key.length()).append("\r\n");
        response.append(key).append("\r\n");

        // Second element: array of entries
        response.append("*").append(entriesAfterStart.size()).append("\r\n");

        // Build each entry
        for (Map<String, String> entry : entriesAfterStart) {
            String entryId = entry.get("id");

            // Each entry is an array of 2 elements: [id, field-value pairs]
            response.append("*2\r\n");

            // First element: entry ID as bulk string
            response.append("$").append(entryId.length()).append("\r\n");
            response.append(entryId).append("\r\n");

            // Second element: array of field-value pairs
            List<String> fieldValuePairs = new ArrayList<>();
            for (Map.Entry<String, String> e : entry.entrySet()) {
                String k = e.getKey();
                String v = e.getValue();
                if (!k.equals("id")) {
                    fieldValuePairs.add(k);
                    fieldValuePairs.add(v);
                }
            }

            // Array size for field-value pairs
            response.append("*").append(fieldValuePairs.size()).append("\r\n");

            // Add each field and value as bulk strings
            for (String item : fieldValuePairs) {
                response.append("$").append(item.length()).append("\r\n");
                response.append(item).append("\r\n");
            }
        }

        return response.toString();
    }

    // Compare two IDs: returns negative if id1 < id2, 0 if equal, positive if id1 > id2
    private int compareIds(String id1, String id2) {
        String[] parts1 = id1.split("-");
        String[] parts2 = id2.split("-");

        long millis1 = Long.parseLong(parts1[0]);
        long millis2 = Long.parseLong(parts2[0]);

        if (millis1 != millis2) {
            return Long.compare(millis1, millis2);
        }

        long seq1 = Long.parseLong(parts1[1]);
        long seq2 = Long.parseLong(parts2[1]);

        return Long.compare(seq1, seq2);
    }
}