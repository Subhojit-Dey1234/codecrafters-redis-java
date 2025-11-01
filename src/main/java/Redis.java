import java.io.*;
import java.net.Socket;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;

public class Redis {

    private final HashMap<String, ValueWithTime> map = new HashMap<>();
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
                        int time = -1;
                        if(commands.length > 3) {
                            time = Integer.parseInt(commands[4]);
                            if(commands[3].equalsIgnoreCase("ex")) time = 1000 * time;
                        }
                        map.put(commands[1], new ValueWithTime(commands[2], Instant.now(), time));
                        sendMessage("+OK\r\n");
                    } else if (redisCommand.equalsIgnoreCase("get")) {
                        Instant now = Instant.now();

                        String key = commands[1];
                        ValueWithTime valueForMap = map.get(key);
                        if(valueForMap != null) {
                            long elapsedMillis = Duration.between(valueForMap.getGetTime(), now).toMillis();
                            if(valueForMap.getTime() != -1 && elapsedMillis >= valueForMap.getTime()){
                                map.remove(key);
                                valueForMap = null;
                            }
                        }

                        if(valueForMap != null)
                            sendMessage("$" + valueForMap.getValue().length() + "\r\n" + valueForMap.getValue() + "\r\n");
                        else
                            sendMessage("$-1\r\n");
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

class ValueWithTime {
    private final String value;
    private final Instant getTime;
    private final long time;

    public ValueWithTime(String value, Instant getTime, long time) {
        this.value = value;
        this.getTime = getTime;
        this.time = time;
    }

    public String getValue() {return value;}
    public Instant getGetTime() {return getTime;}
    public long getTime() {return time;}
}