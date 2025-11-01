import java.io.*;
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
        } catch (IOException ignored) {
            System.exit(-1);
        }
    }

    static void handleRequest(Socket clientSocket) {
        try(clientSocket;
            BufferedWriter outputStream = new BufferedWriter(
                    new OutputStreamWriter(clientSocket.getOutputStream())
            );
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(clientSocket.getInputStream())
            );){
            String content;
            while ((content = in.readLine()) != null) {
                if(content.equalsIgnoreCase("ping")){
                    outputStream.write("+PONG\r\n");
                    outputStream.flush();
                } else if(content.equalsIgnoreCase("echo")){
                    String a = in.readLine();
                    String outputMsg = (a + "\r\n" + in.readLine() + "\r\n");
                    outputStream.write(outputMsg);
                    outputStream.flush();
                }
            }
        }
        catch (IOException ignored) {}
    }


}
