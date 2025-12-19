package redis.commandhelper;

import redis.interfce.IRedisCommandExecutor;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class XAddCommandExecutor implements IRedisCommandExecutor {

    private final Map<String, List<Map<String, String>>> xaddHashMap;

    public XAddCommandExecutor(Map<String, List<Map<String, String>>> xaddHashMap) {
        this.xaddHashMap = xaddHashMap;
    }

    @Override
    public String getMessage(String[] commands) {
        String key = commands[1];
        String id = commands[2];

        String finalId = autoGenerateId(key, id);
        String err = validateId(key, finalId);
        if (err != null) return err;

        List<Map<String, String>> stream =
                xaddHashMap.computeIfAbsent(key, k -> Collections.synchronizedList(new ArrayList<>()));

        Map<String, String> entry = new ConcurrentHashMap<>();
        entry.put("id", finalId);

        for (int i = 3; i < commands.length - 1; i += 2) {
            entry.put(commands[i], commands[i + 1]);
        }

        // 1️⃣ Insert
        stream.add(entry);

        // 2️⃣ Wake blocked XREAD clients
        XReadCommandExecutor.notifyReaders(key);

        return "$" + finalId.length() + "\r\n" + finalId + "\r\n";
    }

    private String autoGenerateId(String key, String id) {
        if ("*".equals(id)) {
            long now = System.currentTimeMillis();
            return now + "-" + generateSeq(key, now);
        }

        if (!id.contains("*")) return id;

        String[] p = id.split("-");
        long millis = Long.parseLong(p[0]);
        return millis + "-" + generateSeq(key, millis);
    }

    private long generateSeq(String key, long millis) {
        List<Map<String, String>> stream = xaddHashMap.get(key);
        if (stream == null || stream.isEmpty()) return millis == 0 ? 1 : 0;

        for (int i = stream.size() - 1; i >= 0; i--) {
            String[] p = stream.get(i).get("id").split("-");
            long m = Long.parseLong(p[0]);
            if (m == millis) return Long.parseLong(p[1]) + 1;
            if (m < millis) break;
        }
        return millis == 0 ? 1 : 0;
    }

    private String validateId(String key, String id) {
        if ("0-0".equals(id)) {
            return "-ERR The ID specified in XADD must be greater than 0-0\r\n";
        }

        String[] p = id.split("-");
        long m = Long.parseLong(p[0]);
        long s = Long.parseLong(p[1]);

        List<Map<String, String>> stream = xaddHashMap.get(key);
        if (stream != null && !stream.isEmpty()) {
            String[] last = stream.get(stream.size() - 1).get("id").split("-");
            long lm = Long.parseLong(last[0]);
            long ls = Long.parseLong(last[1]);

            if (m < lm || (m == lm && s <= ls)) {
                return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
            }
        }
        return null;
    }
}
