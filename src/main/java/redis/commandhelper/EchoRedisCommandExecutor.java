package redis.commandhelper;

import redis.interfce.IRedisCommandExecutor;

public class EchoRedisCommandExecutor implements IRedisCommandExecutor {
    @Override
    public String getMessage(String[] commands) {
        return "$" + commands[1].length() + "\r\n" + commands[1] + "\r\n";
    }
}
