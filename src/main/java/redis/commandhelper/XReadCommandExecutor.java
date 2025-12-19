package redis.commandhelper;

import redis.interfce.IRedisCommandExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.TimeUnit;

public class XReadCommandExecutor implements IRedisCommandExecutor {

    private final Map<String, List<Map<String, String>>> xaddHashMap;
    private final Map<String, List<BlockedClient>> blockedClients = new ConcurrentHashMap<>();
    private final Lock lock = new ReentrantLock();

    public XReadCommandExecutor(Map<String, List<Map<String, String>>> xaddHashMap){
        this.xaddHashMap = xaddHashMap;
    }

    @Override
    public String getMessage(String[] commands) {
        // Parse command: XREAD [BLOCK <ms>] STREAMS <key1> <key2> ... <id1> <id2> ...

        int blockTimeout = -1; // -1 means no blocking
        int streamsIndex = -1;

        // Check for BLOCK option
        for (int i = 0; i < commands.length; i++) {
            if ("BLOCK".equalsIgnoreCase(commands[i]) && i + 1 < commands.length) {
                blockTimeout = Integer.parseInt(commands[i + 1]);
            }
            if ("STREAMS".equalsIgnoreCase(commands[i])) {
                streamsIndex = i;
                break;
            }
        }

        if (streamsIndex == -1) {
            return "-ERR STREAMS keyword not found\r\n";
        }

        // Calculate number of streams
        int totalArgs = commands.length - streamsIndex - 1;
        if (totalArgs % 2 != 0) {
            return "-ERR wrong number of arguments\r\n";
        }

        int numStreams = totalArgs / 2;

        // Extract keys and IDs
        String[] keys = new String[numStreams];
        String[] ids = new String[numStreams];

        for (int i = 0; i < numStreams; i++) {
            keys[i] = commands[streamsIndex + 1 + i];
            ids[i] = commands[streamsIndex + 1 + numStreams + i];
        }

        // Try to get results immediately
        String result = getStreamResults(keys, ids);

        // If we have results or no blocking requested, return immediately
        if (!result.equals("*-1\r\n") || blockTimeout == -1) {
            return result;
        }

        // If blocking is requested and no results, block and wait
        return blockAndWait(keys, ids, blockTimeout);
    }

    private String blockAndWait(String[] keys, String[] ids, int timeoutMs) {
        lock.lock();
        try {
            Condition condition = lock.newCondition();
            BlockedClient client = new BlockedClient(keys, ids, condition);

            // Register this client as blocked for each stream
            for (String key : keys) {
                blockedClients.computeIfAbsent(key, k -> new ArrayList<>()).add(client);
            }

            try {
                // Wait for timeout or notification
                if (timeoutMs == 0) {
                    condition.await(); // Block indefinitely
                } else {
                    condition.await(timeoutMs, TimeUnit.MILLISECONDS);
                }

                // Check if we have results now
                String result = getStreamResults(keys, ids);
                return result;

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return "*-1\r\n";
            } finally {
                // Unregister the client
                for (String key : keys) {
                    List<BlockedClient> clients = blockedClients.get(key);
                    if (clients != null) {
                        clients.remove(client);
                        if (clients.isEmpty()) {
                            blockedClients.remove(key);
                        }
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    // This method should be called when a new entry is added via XADD
    public void notifyBlockedClients(String key) {
        lock.lock();
        try {
            List<BlockedClient> clients = blockedClients.get(key);
            if (clients != null) {
                // Notify all blocked clients waiting on this stream
                for (BlockedClient client : new ArrayList<>(clients)) {
                    client.condition.signal();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private String getStreamResults(String[] keys, String[] ids) {
        // Process each stream and collect results
        List<StreamResult> results = new ArrayList<>();

        for (int i = 0; i < keys.length; i++) {
            String key = keys[i];
            String startId = ids[i];

            List<Map<String, String>> listStream = xaddHashMap.get(key);

            // Skip if stream doesn't exist or is empty
            if (listStream == null || listStream.isEmpty()) {
                continue;
            }

            // Find all entries with ID greater than startId (exclusive)
            List<Map<String, String>> entriesAfterStart = new ArrayList<>();
            for (Map<String, String> entry : listStream) {
                String entryId = entry.get("id");
                if (compareIds(entryId, startId) > 0) {
                    entriesAfterStart.add(entry);
                }
            }

            // Only add to results if there are entries
            if (!entriesAfterStart.isEmpty()) {
                results.add(new StreamResult(key, entriesAfterStart));
            }
        }

        // If no streams have results, return null array
        if (results.isEmpty()) {
            return "*-1\r\n";
        }

        // Build RESP response
        StringBuilder response = new StringBuilder();

        // Outer array: array of streams
        response.append("*").append(results.size()).append("\r\n");

        // Build each stream result
        for (StreamResult streamResult : results) {
            // Each stream is an array of 2 elements: [key, entries]
            response.append("*2\r\n");

            // First element: stream key as bulk string
            response.append("$").append(streamResult.key.length()).append("\r\n");
            response.append(streamResult.key).append("\r\n");

            // Second element: array of entries
            response.append("*").append(streamResult.entries.size()).append("\r\n");

            // Build each entry
            for (Map<String, String> entry : streamResult.entries) {
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

    // Helper class to store stream results
    private static class StreamResult {
        String key;
        List<Map<String, String>> entries;

        StreamResult(String key, List<Map<String, String>> entries) {
            this.key = key;
            this.entries = entries;
        }
    }

    // Helper class to track blocked clients
    private static class BlockedClient {
        String[] keys;
        String[] ids;
        Condition condition;

        BlockedClient(String[] keys, String[] ids, Condition condition) {
            this.keys = keys;
            this.ids = ids;
            this.condition = condition;
        }
    }
}