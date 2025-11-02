package redis.commandhelper;

import redis.dto.ValueWithTime;
import redis.interfce.IRedisCommandExecutor;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

public class TypeRedisCommandExecutor implements IRedisCommandExecutor {

    private final Map<String, ValueWithTime> mapWithTimeOut;
    public TypeRedisCommandExecutor(Map<String, ValueWithTime> mapWithTimeOut){
        this.mapWithTimeOut = mapWithTimeOut;
    }

    @Override
    public String getMessage(String[] commands) {
        Instant now = Instant.now();
        String key = commands[1];
        ValueWithTime valueForMap = mapWithTimeOut.get(key);
        if(valueForMap != null) {
            long elapsedMillis = Duration.between(valueForMap.getGetTime(), now).toMillis();
            if(valueForMap.getTime() != -1 && elapsedMillis >= valueForMap.getTime()){
                mapWithTimeOut.remove(key);
                valueForMap = null;
            }
        }

        if(valueForMap != null)
            return "+string\r\n";
        return "+none\r\n";
    }
}