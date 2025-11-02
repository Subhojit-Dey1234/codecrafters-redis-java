import redis.RedisCommandRegistry;
import redis.dto.ValueWithTime;
import redis.interfce.IRedisCommandExecutor;

import java.io.*;
import java.net.Socket;
import java.util.*;

public class Redis {

    private final BufferedWriter outputStream;
    private final BufferedReader in;
    private final RedisCommandRegistry redisCommandRegistry;

    public Redis(Socket clientSocket, Map<String, List<String>> listHashMap,
                 Map<String, ValueWithTime> valueWithTimeMap,
                 Map<String, List<Map<String, String>>> xaddHashMap) throws IOException {
        this.in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        this.outputStream = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
        this.redisCommandRegistry = new RedisCommandRegistry(listHashMap, valueWithTimeMap, xaddHashMap);
    }

    public void handleRequest() {
        String content;
        var registry = this.redisCommandRegistry.load();
        try {
            while ((content = in.readLine()) != null) {
                if (content.startsWith("*")) {
                    int numberOfCommands = Integer.parseInt(content.substring(1));
                    String[] commands = getStrings(numberOfCommands);
                    String redisCommand = commands[0].toLowerCase().trim();
                    IRedisCommandExecutor redisCommandExecutor = registry.get(redisCommand);
                    if(redisCommandExecutor == null) sendMessage("-ERR unknown command\r\n");
                    else sendMessage(redisCommandExecutor.getMessage(commands));
                }
            }
        } catch (Exception ignored) {
            System.out.println(ignored.getMessage());
        }
    }

    private String[] getStrings(int numberOfCommands) throws IOException {
        String[] commands = new String[numberOfCommands];
        for (int i = 0; i < numberOfCommands; i++) {
            String redisStartOfLine = in.readLine();
            if (!redisStartOfLine.startsWith("$")) throw new RuntimeException("Invalid command");
            String value = in.readLine();
            commands[i] = value;
        }
        return commands;
    }

    private void sendMessage(String message) throws IOException {
        outputStream.write(message);
        outputStream.flush();
    }
}