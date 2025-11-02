package redis.commandhelper;

import redis.interfce.IRedisCommandExecutor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class LLenCommandExecutor implements IRedisCommandExecutor {

    private final Map<String, List<String>> hashMapWithListString;

    public LLenCommandExecutor(Map<String, List<String>> hashMapWithListString){
        this.hashMapWithListString = hashMapWithListString;
    }

    @Override
    public String getMessage(String[] commands) {
        String key = commands[1];
        return ":"+
                hashMapWithListString.getOrDefault(key, Collections.synchronizedList(new ArrayList<>())).size()
                +"\r\n";
    }
}
