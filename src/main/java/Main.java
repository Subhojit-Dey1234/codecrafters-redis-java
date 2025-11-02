import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Main {
    static void main(String[] args) {
        int port = 6379;
        try {
            ServerSocket serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);
            Map<String, List<String>> listHashMap = new ConcurrentHashMap<>();
            while (true) {
                Socket finalSocket = serverSocket.accept();
                Redis redis = new Redis(finalSocket, listHashMap, new ConcurrentHashMap<>());
                new Thread(redis::handleRequest).start();
            }
        } catch (IOException ignored) {
            System.exit(-1);
        }
    }
}
