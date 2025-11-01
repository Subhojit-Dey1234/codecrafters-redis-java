import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

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
        } catch (IOException ignored) {
            System.exit(-1);
        }
    }

    static void handleRequest(Socket clientSocket) {
        try(clientSocket;
            OutputStream outputStream = clientSocket.getOutputStream();
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(clientSocket.getInputStream(), StandardCharsets.UTF_8)
            )){

            while (true) {
                String content = in.readLine();
                if(in.readLine() == null) break;
                if(content.equalsIgnoreCase("ping")){
                    outputStream.write("+PONG\r\n".getBytes());
                    outputStream.flush();
                } else if(content.equalsIgnoreCase("echo")){
                    String numBytes = in.readLine();
                    outputStream.write((numBytes + "\r\n" + in.readLine() + "\r\n").getBytes());
                    outputStream.flush();
                }
            }
        }
        catch (IOException ignored) {}
    }


}
