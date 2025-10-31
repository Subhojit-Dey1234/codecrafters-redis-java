import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
  public static void main(String[] args){
    System.out.println("Logs from your program will appear here!");
        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        int port = 6379;
        try {
          serverSocket = new ServerSocket(port);
          serverSocket.setReuseAddress(true);
          clientSocket = serverSocket.accept();

          while (true){
              byte[] buffer = new byte[1024];
              int read = clientSocket.getInputStream().read(buffer);
              String message = new String(buffer, 0, read).trim();
              System.out.println("Received: " + message);
              clientSocket.getOutputStream().write("+PONG\r\n".getBytes());
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
