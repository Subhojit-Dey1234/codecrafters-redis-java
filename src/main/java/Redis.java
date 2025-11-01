import java.io.*;
import java.net.Socket;
import java.util.HashMap;

public class Redis {

    private final HashMap<String, String> map = new HashMap<>();
    private final BufferedWriter outputStream;
    private final BufferedReader in;

    public Redis(Socket clientSocket) throws IOException {
        this.in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        this.outputStream = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
    }

    public void handleRequest() {
        String content;
        try {
            while ((content = in.readLine()) != null) {
                if (content.startsWith("*")) {
                    int numberOfCommands = Integer.parseInt(content.substring(1));
                    String[] commands = getStrings(numberOfCommands);
                    String redisCommand = commands[0];
                    if (redisCommand.equalsIgnoreCase("ping")) {
                        sendMessage("+PONG\r\n");
                    } else if (redisCommand.equalsIgnoreCase("echo")) {
                        sendMessage("$" + commands[1].length() + "\r\n" + commands[1] + "\r\n");
                    } else if (redisCommand.equalsIgnoreCase("set")) {
                        map.put(commands[1], commands[2]);
                        sendMessage("+OK\r\n");
                    } else if (redisCommand.equalsIgnoreCase("get")) {
                        String key = commands[1];
                        String value = map.getOrDefault(key, "");
                        sendMessage("$" + value.length() + "\r\n" + value + "\r\n");
                    }
                }
            }
        } catch (Exception ignored) {}
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


enum RedisCommand {
    PING("ping", "+", 0), ECHO("echo", "$", 1), GET("get", "+", 1), SET("set", "$", 2);

    private String command;
    private String startChars;
    private int numberOfCommands;

    RedisCommand(String command, String startChars, int numberOfCommands) {
        this.command = command;
        this.startChars = startChars;
        this.numberOfCommands = numberOfCommands;
    }

    public static RedisCommand getCommand(String command) {
        for (RedisCommand redisCommand : RedisCommand.values()) {
            if (redisCommand.command.equalsIgnoreCase(command)) {
                return redisCommand;
            }
        }
        throw new RuntimeException("Redis Invalid Command");
    }
}
