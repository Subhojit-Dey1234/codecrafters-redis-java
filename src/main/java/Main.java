import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class Main {
    public static void main(String[] args) {
        System.out.println("Logs from your program will appear here!");
        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        int port = 6379;
        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);
            while (true) {
                clientSocket = serverSocket.accept();
                Socket finalClientSocket1 = clientSocket;
                new Thread(() -> {
                    byte[] buffer = new byte[1024];
                    Socket finalClientSocket = finalClientSocket1;
                    int read = 0;
                    try {
                        read = finalClientSocket.getInputStream().read(buffer);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    String message = new String(buffer, 0, read).trim();
                    System.out.println("Received: " + message);
                    try {
                        finalClientSocket.getOutputStream().write("+PONG\r\n".getBytes());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }).start();
            }

        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            }
        }
    }
}
