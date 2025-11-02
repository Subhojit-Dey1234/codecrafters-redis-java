package redis.commandhelper;

import redis.dto.ValueWithTime;
import redis.interfce.IRedisCommandExecutor;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

public class GetRedisCommandExecutor implements IRedisCommandExecutor {

    private final Map<String, ValueWithTime> mapWithTimeOut;

    public GetRedisCommandExecutor(Map<String, ValueWithTime> mapWithTimeOut){
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
            return "$" + valueForMap.getValue().length() + "\r\n" + valueForMap.getValue() + "\r\n";
        return "$-1\r\n";
    }
}
