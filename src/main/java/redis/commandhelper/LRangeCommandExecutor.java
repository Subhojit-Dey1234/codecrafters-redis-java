package redis.commandhelper;

import redis.interfce.IRedisCommandExecutor;

import java.util.List;
import java.util.Map;

public class LRangeCommandExecutor implements IRedisCommandExecutor {

    private final Map<String, List<String>> hashMapWithListString;

    public LRangeCommandExecutor(Map<String, List<String>> hashMapWithListString){
        this.hashMapWithListString = hashMapWithListString;
    }

    @Override
    public String getMessage(String[] commands) {
        String key = commands[1];
        List<String> list = hashMapWithListString.getOrDefault(key, List.of());

        int startInd = Integer.parseInt(commands[2]);
        int endInd = Integer.parseInt(commands[3]);

        if (startInd < 0) {
            startInd = list.size() + startInd;
            if (startInd < 0) startInd = 0;
        }
        if (endInd < 0) {
            endInd = list.size() + endInd;
            if (endInd < 0) endInd = 0;
        }

        if(startInd > endInd) return "*0\r\n";
        StringBuilder builder = new StringBuilder();
        int cnt = 0;
        for(int i = startInd; i <= Math.min(endInd, list.size()-1); i++){
            cnt ++;
            String value = list.get(i);
            builder.append("$").append(value.length()).append("\r\n");
            builder.append(value).append("\r\n");
        }
        String cntString = "*" + cnt + "\r\n";
        return cntString + builder;
    }
}
