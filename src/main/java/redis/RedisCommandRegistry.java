package redis;

import redis.commandhelper.*;
import redis.dto.ValueWithTime;
import redis.interfce.IRedisCommandExecutor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RedisCommandRegistry {
    private final Map<String, List<String>> hashMapWithListString;
    private final Map<String, ValueWithTime> valueWithTimeMap;
    private final Map<String, List<Map<String, String>>> xaddHashMap;

    public RedisCommandRegistry(Map<String, List<String>> hashMapWithListString,
                                Map<String, ValueWithTime> valueWithTimeMap,
                                Map<String, List<Map<String, String>>> xaddHashMap){
        this.hashMapWithListString = hashMapWithListString;
        this.valueWithTimeMap = valueWithTimeMap;
        this.xaddHashMap = xaddHashMap;
    }

    public Map<String, IRedisCommandExecutor> load(){
        Map<String, IRedisCommandExecutor> commandExecutorMap = new ConcurrentHashMap<>();
        commandExecutorMap.put("ping", new PingRedisCommandExecutor());
        commandExecutorMap.put("echo", new EchoRedisCommandExecutor());
        commandExecutorMap.put("get", new GetRedisCommandExecutor(valueWithTimeMap));
        commandExecutorMap.put("set", new SetRedisCommandExecutor(valueWithTimeMap));
        commandExecutorMap.put("rpush", new RPushCommandExecutor(hashMapWithListString));
        commandExecutorMap.put("lpush", new LPushCommandExecutor(hashMapWithListString));
        commandExecutorMap.put("llen", new LLenCommandExecutor(hashMapWithListString));
        commandExecutorMap.put("lpop", new LPopCommandExecutor(hashMapWithListString));
        commandExecutorMap.put("lrange", new LRangeCommandExecutor(hashMapWithListString));
        commandExecutorMap.put("blpop", new BLPopCommandExecutor(hashMapWithListString));
        commandExecutorMap.put("type", new TypeRedisCommandExecutor(valueWithTimeMap, xaddHashMap));
        commandExecutorMap.put("xadd", new XAddCommandExecutor(xaddHashMap));
        commandExecutorMap.put("xrange", new XRangeCommandExecutor(xaddHashMap));
        commandExecutorMap.put("xread", new XReadCommandExecutor(xaddHashMap));

        return commandExecutorMap;
    }
}
