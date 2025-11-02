package redis.commandhelper;

import redis.interfce.IRedisCommandExecutor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class LPopCommandExecutor implements IRedisCommandExecutor {

    private final Map<String, List<String>> hashMapWithListString;

    public LPopCommandExecutor(Map<String, List<String>> hashMapWithListString){
        this.hashMapWithListString = hashMapWithListString;
    }

    @Override
    public String getMessage(String[] commands) {
        String key = commands[1];
        int index = -1;
        if( commands.length > 2 ) index = Integer.parseInt(commands[2]);
        List<String> lst = hashMapWithListString.getOrDefault(key, Collections.synchronizedList(new ArrayList<>()));
        if(lst.isEmpty()) return "$-1\r\n";

        if(index < 0){
            String value = lst.getFirst();
            lst.removeFirst();
            return "$" + value.length() + "\r\n" + value + "\r\n";
        }

        StringBuilder builder = new StringBuilder();
        int cnt = 0;
        while (cnt < index && !lst.isEmpty()){
            String value = lst.getFirst();
            builder.append("$").append(value.length()).append("\r\n");
            builder.append(value).append("\r\n");
            lst.removeFirst();
            cnt ++;
        }
        String cntString = "*" + cnt + "\r\n";
        return cntString + builder;
    }
}
