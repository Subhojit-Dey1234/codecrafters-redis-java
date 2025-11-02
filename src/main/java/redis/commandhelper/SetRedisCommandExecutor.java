package redis.commandhelper;

import redis.dto.ValueWithTime;
import redis.interfce.IRedisCommandExecutor;

import java.time.Instant;
import java.util.Map;

public class SetRedisCommandExecutor implements IRedisCommandExecutor {
    private final Map<String, ValueWithTime> map;
    public SetRedisCommandExecutor(Map<String, ValueWithTime> listHashMap){
        this.map = listHashMap;
    }

    @Override
    public String getMessage(String[] commands) {
        int time = -1;
        if(commands.length > 3) {
            time = Integer.parseInt(commands[4]);
            if(commands[3].equalsIgnoreCase("ex")) time = 1000 * time;
        }
        map.put(commands[1], new ValueWithTime(commands[2], Instant.now(), time));
        return "+OK\r\n";
    }
}