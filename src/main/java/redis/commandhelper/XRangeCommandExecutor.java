package redis.commandhelper;

import redis.interfce.IRedisCommandExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class XRangeCommandExecutor implements IRedisCommandExecutor {

    private final Map<String, List<Map<String, String>>> xaddHashMap;

    public XRangeCommandExecutor(Map<String, List<Map<String, String>>> xaddHashMap){
        this.xaddHashMap = xaddHashMap;
    }

    @Override
    public String getMessage(String[] commands) {
        String key = commands[1];
        String startId = normalizeStartId(commands[2]);
        String endId = normalizeEndId(commands[3]);

        List<Map<String, String>> lst = xaddHashMap.get(key);

        if (lst == null || lst.isEmpty()) {
            return "*0\r\n";
        }

        List<Map<String, String>> entriesInRange = new ArrayList<>();
        for (Map<String, String> entry : lst) {
            String entryId = entry.get("id");
            if (isInRange(entryId, startId, endId)) {
                entriesInRange.add(entry);
            }
        }

        StringBuilder response = new StringBuilder();
        response.append("*").append(entriesInRange.size()).append("\r\n");

        for (Map<String, String> entry : entriesInRange) {
            String entryId = entry.get("id");
            response.append("*2\r\n");
            response.append("$").append(entryId.length()).append("\r\n");
            response.append(entryId).append("\r\n");

            List<String> fieldValuePairs = new ArrayList<>();
            for (Map.Entry<String, String> e : entry.entrySet()) {
                String k = e.getKey();
                String v = e.getValue();
                if (!k.equals("id")) {
                    fieldValuePairs.add(k);
                    fieldValuePairs.add(v);
                }
            }

            response.append("*").append(fieldValuePairs.size()).append("\r\n");
            for (String item : fieldValuePairs) {
                response.append("$").append(item.length()).append("\r\n");
                response.append(item).append("\r\n");
            }
        }

        return response.toString();
    }

    private String normalizeStartId(String id) {
        if(id.equals("-")){
            return "0-0";
        }
        if (!id.contains("-")) {
            return id + "-0";
        }
        return id;
    }

    private String normalizeEndId(String id) {
        if (!id.contains("-")) {
            return id + "-" + Long.MAX_VALUE;
        }
        return id;
    }

    private boolean isInRange(String entryId, String startId, String endId) {
        return compareIds(entryId, startId) >= 0 && compareIds(entryId, endId) <= 0;
    }

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