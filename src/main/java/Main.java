import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
    static void main(String[] args) {
        System.out.println("Logs from your program will appear here!");
        int port = 6379;
        try {
            ServerSocket serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);
            while (true) {
                Socket finalSocket = serverSocket.accept();
                new Thread(() -> handleRequest(finalSocket)).start();
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    static void handleRequest(Socket clientSocket) {
        try(clientSocket;
            OutputStream outputStream = clientSocket.getOutputStream();
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(clientSocket.getInputStream())
            )){

            while (true) {
                if(in.readLine() == null) break;
                String line = in.readLine();
                System.out.println(line);
                outputStream.write("+PONG\r\n".getBytes());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}
