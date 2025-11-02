import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
    static void main(String[] args) {
        int port = 6379;
        try {
            ServerSocket serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);
            while (true) {
                Socket finalSocket = serverSocket.accept();
                Redis redis = new Redis(finalSocket);
                Thread.ofVirtual().start(redis::handleRequest);
            }
        } catch (IOException ignored) {
            System.exit(-1);
        }
    }
}
