package redis.commandhelper;

import redis.dto.ValueWithTime;
import redis.interfce.IRedisCommandExecutor;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public class TypeRedisCommandExecutor implements IRedisCommandExecutor {

    private final Map<String, ValueWithTime> mapWithTimeOut;
    private final Map<String, List<Map<String, String>>> xaddHashMap;

    public TypeRedisCommandExecutor(
            Map<String, ValueWithTime> mapWithTimeOut,
            Map<String, List<Map<String, String>>> xaddHashMap){
        this.mapWithTimeOut = mapWithTimeOut;
        this.xaddHashMap = xaddHashMap;
    }

    @Override
    public String getMessage(String[] commands) {
        String key = commands[1];
        var valueForMap = mapWithTimeOut.get(key);
        var xaddObj = xaddHashMap.get(key);
        if(valueForMap != null)
            return "+string\r\n";
        if(xaddObj != null) return "+stream\r\n";
        return "+none\r\n";
    }
}