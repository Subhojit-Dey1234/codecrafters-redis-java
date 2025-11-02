package redis.commandhelper;

import redis.interfce.IRedisCommandExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RPushCommandExecutor implements IRedisCommandExecutor {

    private final Map<String, List<String>> hashMapWithListString;

    public RPushCommandExecutor(Map<String, List<String>> hashMapWithListString){
        this.hashMapWithListString = hashMapWithListString;
    }


    @Override
    public String getMessage(String[] commands) {
        String key = commands[1];
        for(int i = 2; i < commands.length; i++){
            hashMapWithListString.computeIfAbsent(key, (_) -> new ArrayList<>()).add(commands[i]);
        }
        return ":"+ hashMapWithListString.get(key).size() +"\r\n";
    }
}
