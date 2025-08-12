import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

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
  private static ConcurrentHashMap<String, List<String>> map = new ConcurrentHashMap<>();
  private static ConcurrentHashMap<String, List<String>> rpushMap = new ConcurrentHashMap<>();

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
          System.out.println(line);
          line = line.trim();

          System.out.println("Received: " + line);

          List<String> lst = parseRedisQuery(line, in);
          System.out.println(lst);

          String response = getResponse(lst);
          if(response!=null && !response.isEmpty()){
            System.out.println("Response : " + response);
            out.write(response+"\r\n");
          }
          out.flush();
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

  public static String getResponse(List<String> requestList) throws IOException {

    String response = "";

    if(!requestList.isEmpty()){

      if(requestList.get(0).equals("PING")){
        response = "+PONG";
      }

      if(requestList.size()>1 && requestList.get(0).equals("ECHO")){

        // RESP simple string response
        response = "+" + requestList.get(1);
      }

      if(requestList.size()==3 && requestList.get(0).equals("SET")){
        map.put(requestList.get(1), Arrays.asList(requestList.get(2)));
        return "+OK";
      }

      if(requestList.size()==5 && requestList.get(0).equals("SET") && requestList.get(3).equals("px")){
        int timeout = Integer.parseInt(requestList.get(4));
        map.put(requestList.get(1), Arrays.asList(requestList.get(2), LocalTime.now().plus(timeout, ChronoUnit.MILLIS).toString()));
        return "+OK";
      }

      if(requestList.size()==2 && requestList.get(0).equals("GET")){
        System.out.println("Received Get Request");
        System.out.println(map.toString());
        List<String> res = map.getOrDefault(requestList.get(1), new ArrayList<>());
        if(!res.isEmpty()){
          if(res.size() == 1){
            return "+"+res.get(0);
          }else if(LocalTime.now().isBefore(LocalTime.parse(res.get(1)))){
            return "+"+res.get(0);
          }else {
            return "$-1";
          }
        }
        return null;
      }

      if(requestList.size() >= 3 && requestList.get(0).equals("RPUSH")){
        for(int i=2; i<requestList.size(); i++){

          rpushMap.computeIfAbsent(requestList.get(1), k->new ArrayList<>()).add(requestList.get(i));
        }
        return ":"+String.valueOf(rpushMap.get(requestList.get(1)).size());
      }


    }



    System.out.println("resp ; " + response);
    return response;
  }

  public List<String> parseRedisQuery(String rq, BufferedReader in) throws IOException {

//    rq will be *n where n will be number of strings we have to read

    int n = Integer.parseInt(rq.substring(1));
    List<String> lst = new ArrayList<>();
    for(int i=0; i<n; i++){
      int lineLen = Integer.parseInt(in.readLine().substring(1));
      lst.add(in.readLine());
    }

    return lst;

  }

}

