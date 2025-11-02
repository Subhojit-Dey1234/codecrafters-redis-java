package redis.commandhelper;

import redis.interfce.IRedisCommandExecutor;

public class PingRedisCommandExecutor implements IRedisCommandExecutor {
    @Override
    public String getMessage(String[] commands) {
        return "+PONG\r\n";
    }
}
