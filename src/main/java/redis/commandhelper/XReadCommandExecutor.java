package redis.commandhelper;

import redis.interfce.IRedisCommandExecutor;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class XReadCommandExecutor implements IRedisCommandExecutor {

    // Shared across ALL executors (important)
    private static final Lock LOCK = new ReentrantLock();
    private static final Map<String, List<Condition>> WAITERS = new ConcurrentHashMap<>();

    private final Map<String, List<Map<String, String>>> xaddHashMap;

    public XReadCommandExecutor(Map<String, List<Map<String, String>>> xaddHashMap) {
        this.xaddHashMap = xaddHashMap;
    }

    @Override
    public String getMessage(String[] commands) {
        int blockTimeout = -1;
        int streamsIndex = -1;

        for (int i = 0; i < commands.length; i++) {
            if ("BLOCK".equalsIgnoreCase(commands[i])) {
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

        int totalArgs = commands.length - streamsIndex - 1;
        if (totalArgs % 2 != 0) {
            return "-ERR wrong number of arguments\r\n";
        }

        int n = totalArgs / 2;
        String[] keys = new String[n];
        String[] ids = new String[n];

        for (int i = 0; i < n; i++) {
            keys[i] = commands[streamsIndex + 1 + i];
            ids[i] = commands[streamsIndex + 1 + n + i];
        }

        String immediate = getStreamResults(keys, ids);
        if (!"*-1\r\n".equals(immediate) || blockTimeout == -1) {
            return immediate;
        }

        return blockAndWait(keys, ids, blockTimeout);
    }

    private String blockAndWait(String[] keys, String[] ids, int timeoutMs) {
        Condition condition = null;
        LOCK.lock();
        try {
            condition = LOCK.newCondition();

            for (String key : keys) {
                WAITERS.computeIfAbsent(key, k -> new ArrayList<>()).add(condition);
            }

            long deadline = timeoutMs > 0
                    ? System.currentTimeMillis() + timeoutMs
                    : 0;

            while (true) {
                String result = getStreamResults(keys, ids);
                if (!"*-1\r\n".equals(result)) {
                    return result;
                }

                if (timeoutMs == 0) {
                    condition.await(); // BLOCK forever
                } else {
                    long remaining = deadline - System.currentTimeMillis();
                    if (remaining <= 0) {
                        return "*-1\r\n";
                    }
                    condition.await(remaining, TimeUnit.MILLISECONDS);
                }
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "*-1\r\n";
        } finally {
            for (String key : keys) {
                List<Condition> list = WAITERS.get(key);
                if (list != null) {
                    list.remove(condition);
                    if (list.isEmpty()) {
                        WAITERS.remove(key);
                    }
                }
            }
            LOCK.unlock();
        }
    }

    // Called by XADD
    public static void notifyReaders(String key) {
        LOCK.lock();
        try {
            List<Condition> waiters = WAITERS.get(key);
            if (waiters != null) {
                for (Condition c : waiters) {
                    c.signal();
                }
            }
        } finally {
            LOCK.unlock();
        }
    }

    private String getStreamResults(String[] keys, String[] ids) {
        List<StreamResult> results = new ArrayList<>();

        for (int i = 0; i < keys.length; i++) {
            List<Map<String, String>> stream = xaddHashMap.get(keys[i]);
            if (stream == null) continue;

            List<Map<String, String>> entries = new ArrayList<>();
            for (Map<String, String> entry : stream) {
                if (compareIds(entry.get("id"), ids[i]) > 0) {
                    entries.add(entry);
                }
            }

            if (!entries.isEmpty()) {
                results.add(new StreamResult(keys[i], entries));
            }
        }

        if (results.isEmpty()) {
            return "*-1\r\n";
        }

        StringBuilder resp = new StringBuilder();
        resp.append("*").append(results.size()).append("\r\n");

        for (StreamResult sr : results) {
            resp.append("*2\r\n");
            resp.append("$").append(sr.key.length()).append("\r\n").append(sr.key).append("\r\n");
            resp.append("*").append(sr.entries.size()).append("\r\n");

            for (Map<String, String> entry : sr.entries) {
                resp.append("*2\r\n");
                String id = entry.get("id");
                resp.append("$").append(id.length()).append("\r\n").append(id).append("\r\n");

                List<String> fv = new ArrayList<>();
                for (Map.Entry<String, String> e : entry.entrySet()) {
                    if (!"id".equals(e.getKey())) {
                        fv.add(e.getKey());
                        fv.add(e.getValue());
                    }
                }

                resp.append("*").append(fv.size()).append("\r\n");
                for (String s : fv) {
                    resp.append("$").append(s.length()).append("\r\n").append(s).append("\r\n");
                }
            }
        }

        return resp.toString();
    }

    private int compareIds(String a, String b) {
        String[] p1 = a.split("-");
        String[] p2 = b.split("-");
        int c = Long.compare(Long.parseLong(p1[0]), Long.parseLong(p2[0]));
        return c != 0 ? c : Long.compare(Long.parseLong(p1[1]), Long.parseLong(p2[1]));
    }

    private static class StreamResult {
        String key;
        List<Map<String, String>> entries;

        StreamResult(String key, List<Map<String, String>> entries) {
            this.key = key;
            this.entries = entries;
        }
    }
}
