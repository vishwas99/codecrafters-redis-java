import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
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
          clientSocket = serverSocket.accept();

          while(true){
//            byte[] input = new byte[1024];
//            clientSocket.getInputStream().read(input);
//            if(inpu)
            SocketHandler socketHandler = new SocketHandler(serverSocket, clientSocket);
            pool.execute(socketHandler);
//            String inputString = new String(input).trim();
//            System.out.println(inputString);
//            OutputStream outputStream = clientSocket.getOutputStream();
//            outputStream.write("+PONG\r\n".getBytes());
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

class SocketHandler implements Runnable{

  ServerSocket serverSocket = null;
  Socket clientSocket = null;

  public SocketHandler(ServerSocket serverSocket, Socket clientSocket) throws IOException {
    System.out.println("Called new Socket Thread");
    this.serverSocket = serverSocket;
    this.clientSocket = clientSocket;
  }

  public void run(){
    while(true) {
      byte[] input = new byte[1024];
      try {
        clientSocket.getInputStream().read(input);
        String inputString = new String(input).trim();
        System.out.println(inputString);
        OutputStream outputStream = clientSocket.getOutputStream();
        outputStream.write("+PONG\r\n".getBytes());
      }catch(Exception e){
          e.printStackTrace();
      }
    }
  }

}

