import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
  public static void main(String[] args) {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");

    //  Uncomment this block to pass the first stage
        int port = 6379;
        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        ExecutorService pool = Executors.newFixedThreadPool(4);
    try {
          serverSocket = new ServerSocket(port);
          // Since the tester restarts your program quite often, setting SO_REUSEADDR
          // ensures that we don't run into 'Address already in use' errors
          serverSocket.setReuseAddress(true);
          // Wait for connection from client.

          while(true){
            clientSocket = serverSocket.accept();
            new Thread(new SocketHandler(serverSocket,clientSocket)).start();
          }

        } catch (SocketException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

class SocketHandler implements Runnable{

  private final ServerSocket serverSocket;
  private  final Socket clientSocket;

  public SocketHandler(ServerSocket serverSocket, Socket clientSocket) throws IOException {
    System.out.println("Called new Socket Thread");
    this.serverSocket = serverSocket;
    this.clientSocket = clientSocket;
  }

  public void run(){
      try(
              BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
              BufferedWriter out = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
      ) {
        String line;
        // Keep reading until client disconnects
        while ((line = in.readLine()) != null) {
          line = line.trim();
          System.out.println("Received: " + line);

          // Respond only for "PING"
          if (line.equalsIgnoreCase("PING")) {
            out.write("+PONG\r\n");
            out.flush();
          }
        }
      }catch(Exception e){
          e.printStackTrace();
      }finally {
        try {
          clientSocket.close();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
  }

}

