import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class Main {

    public static void handleClient(Socket clientSocket) throws IOException {
        byte[] buffer = new byte[1024];
        int read = clientSocket.getInputStream().read(buffer);
        String message = new String(buffer, 0, read).trim();
        System.out.println("Received: " + message);
        clientSocket.getOutputStream().write("+PONG\r\n".getBytes());
    }

  public static void main(String[] args){
    System.out.println("Logs from your program will appear here!");
        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        int port = 6379;
        try {
          serverSocket = new ServerSocket(port);
          serverSocket.setReuseAddress(true);

          while (true){
              clientSocket = serverSocket.accept();
              Socket finalClientSocket = clientSocket;
              new Thread(()->{
                  try {
                      handleClient(finalClientSocket);
                  } catch (IOException e) {
                      throw new RuntimeException(e);
                  }
              }).start();
          }

        } catch (IOException ignored) {} finally {
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
