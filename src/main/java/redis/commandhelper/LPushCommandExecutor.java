package redis.commandhelper;

import redis.interfce.IRedisCommandExecutor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class LPushCommandExecutor implements IRedisCommandExecutor {

    private final Map<String, List<String>> hashMapWithListString;

    public LPushCommandExecutor(Map<String, List<String>> hashMapWithListString){
        this.hashMapWithListString = hashMapWithListString;
    }


    @Override
    public String getMessage(String[] commands) {
        String key = commands[1];
        List<String> lst = hashMapWithListString.computeIfAbsent(key,
                _ -> Collections.synchronizedList(new ArrayList<>())
        );

        for(int i = 2; i < commands.length; i++) {
            lst.addFirst(commands[i]);
        }
        return ":" + lst.size() + "\r\n";
    }
}
