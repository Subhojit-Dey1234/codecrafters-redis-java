import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    public static void main(String[] args) {
        int port = 6379;
        boolean listening = true;

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);

            while (listening) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("New client connected");
                executor.submit(()-> handleClient(clientSocket));
            }

        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
            System.exit(-1);
        }
    }

    static void handleClient(Socket clientSocket) {
        try (
                clientSocket; // automatically closes socket at the end
                OutputStream outputStream = clientSocket.getOutputStream();
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(clientSocket.getInputStream()))
        ) {

            while (true) {
                // hacky way to read input; will change
                if (in.readLine() == null) {
                    break;
                }
                in.readLine();
                String line = in.readLine();
                System.out.println("Last line: " + line);
                outputStream.write("+PONG\r\n".getBytes());
            }


        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }
}