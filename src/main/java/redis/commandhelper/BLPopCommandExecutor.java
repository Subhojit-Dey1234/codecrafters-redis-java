package redis.commandhelper;

import redis.interfce.IRedisCommandExecutor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class BLPopCommandExecutor implements IRedisCommandExecutor {

    private final Map<String, List<String>> hashMapWithListString;
    private static final long POLL_INTERVAL_MS = 10;

    public BLPopCommandExecutor(Map<String, List<String>> hashMapWithListString){
        this.hashMapWithListString = hashMapWithListString;
    }

    @Override
    public String getMessage(String[] commands) {
        String key = commands[1];
        long timeoutMs = (long) (Double.parseDouble(commands[2]) * 1000);
        long startTime = System.currentTimeMillis();

        while (true) {
            List<String> lst = hashMapWithListString.computeIfAbsent(key,
                    _ -> Collections.synchronizedList(new ArrayList<>())
            );

            synchronized (lst) {
                if (!lst.isEmpty()) {
                    String value = lst.removeFirst();
                    return formatResponse(key, value);
                }

                // Check timeout
                if (timeoutMs > 0) {
                    long elapsed = System.currentTimeMillis() - startTime;
                    if (elapsed >= timeoutMs) {
                        return "*-1\r\n";
                    }
                }
            }

            // Sleep to avoid busy-waiting
            try {
                Thread.sleep(POLL_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return "*-1\r\n";
            }
        }
    }

    private String formatResponse(String key, String value) {
        return String.format("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
                key.length(), key, value.length(), value);
    }
}
