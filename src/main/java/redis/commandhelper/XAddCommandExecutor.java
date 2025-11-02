package redis.commandhelper;

import redis.interfce.IRedisCommandExecutor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class XAddCommandExecutor implements IRedisCommandExecutor {

    private final Map<String, List<Map<String, String>>> xaddHashMap;

    public XAddCommandExecutor(
            Map<String, List<Map<String, String>>> xaddHashMap){
        this.xaddHashMap = xaddHashMap;
    }


    @Override
    public String getMessage(String[] commands) {
        String key = commands[1];
        String id = commands[2];

        List<Map<String, String>> lst = this.xaddHashMap.computeIfAbsent(key,
                _ -> Collections.synchronizedList(new ArrayList<>())
        );

        Map<String, String> mp = new ConcurrentHashMap<>();
        mp.put("id", id);
        for(int i = 3; i < commands.length-1; i+= 2){
            mp.put(commands[i], commands[i+1]);
        }
        lst.add(mp);
        return "$" + id.length() + "\r\n" + id + "\r\n";
    }
}
