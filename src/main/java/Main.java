import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
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
  private static ConcurrentHashMap<String, BlockingDeque<String>> rpushMap = new ConcurrentHashMap<>();

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
        List<String> list = new ArrayList<>();
        Collections.addAll(list, requestList.get(2));
        map.put(requestList.get(1), list);
        return "+OK";
      }

      if(requestList.size()==5 && requestList.get(0).equals("SET") && requestList.get(3).equals("px")){
        int timeout = Integer.parseInt(requestList.get(4));
        List<String> timeoutList = new ArrayList<>();
        Collections.addAll(timeoutList, requestList.get(2), LocalTime.now().plus(timeout, ChronoUnit.MILLIS).toString());

        map.put(requestList.get(1), timeoutList);
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

          rpushMap.computeIfAbsent(requestList.get(1), k->new LinkedBlockingDeque<>()).add(requestList.get(i));
        }
        return ":"+String.valueOf(rpushMap.get(requestList.get(1)).size());
      }

      if(requestList.get(0).equals("LRANGE")){
        return encodeToQuery(rpushMap.getOrDefault(requestList.get(1), new LinkedBlockingDeque<>()).stream().toList(), Integer.parseInt(requestList.get(2)), Integer.parseInt(requestList.get(3)));
      }

      if(requestList.size()>=3 && requestList.get(0).equals("LPUSH")){
        for(int i=2; i<requestList.size(); i++){

          rpushMap.computeIfAbsent(requestList.get(1), k->new LinkedBlockingDeque<>()).addFirst(requestList.get(i));
        }
        return ":"+String.valueOf(rpushMap.get(requestList.get(1)).size());
      }

      if(requestList.size()==2 && requestList.get(0).equals("LLEN")){
        return ":" + String.valueOf(rpushMap.getOrDefault(requestList.get(1), new LinkedBlockingDeque<>()).size());
      }

      if(requestList.size()>=2 && requestList.get(0).equals("LPOP")){
        BlockingDeque<String> targetList = rpushMap.getOrDefault(requestList.get(1), new LinkedBlockingDeque<>());
        if(targetList.size() == 0){
          return "$-1";
        }else if(requestList.size()==2){
          String poppedString = targetList.poll();
          return encodeString(poppedString);
        }else{
          int toRemove = Integer.parseInt(requestList.get(2));
          List<String> responseList = new ArrayList<>();
          while(toRemove > 0 && targetList.size()>0){
            responseList.add(targetList.peek());
            targetList.pop();
            toRemove--;
          }
          return encodeToQuery(responseList, 0, responseList.size()-1);
        }
      }

      if(requestList.get(0).equals("BLPOP")){

        String key = requestList.get(1);
        int timeout = 0;
        if(requestList.size()>2){
          timeout = Integer.parseInt(requestList.get(2));
        }

        rpushMap.computeIfAbsent(key, k->new LinkedBlockingDeque<>());
        BlockingDeque<String> blockingDeque = rpushMap.get(key);

        System.out.println("BLPOP Received at " + LocalTime.now());
        try {
          String value;
          if (timeout == 0) {
            // Block indefinitely until element available
            value = blockingDeque.take();
          } else {
            // Block until timeout
            value = blockingDeque.poll(timeout, TimeUnit.SECONDS);
          }

          if (value != null) {
            String encodedString = encodeToQuery(Arrays.asList(key, value), 0, 1);
            System.out.println(encodedString);
            return encodedString;
          } else {
            // Timed out, send nil
            return "$-1\r\n";
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return "$-1\r\n"; // Treat interruption as nil
        }

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

  public static String encodeToQuery(List<String> list, int start, int end){

    System.out.println("Encode To Query : " + list);

    if(start < 0){
      start = list.size()+start;
      if(start<0){
        start = 0;
      }
    }

    if(end < 0){
      end = list.size()+end;
    }

    StringBuilder sb = new StringBuilder();

    sb.append("*");
    if(start > end || list.size() == 0){
      sb.append(0);
      return sb.toString();
    }
    int newStart = Math.min(start, list.size()-1);
    newStart = Math.max(0, start);
    int newEnd = Math.min(end, list.size()-1);
    newEnd = Math.max(0, newEnd);
    sb.append(newEnd-newStart+1);
    sb.append("\r\n");

    for (int i=newStart; i<=newEnd && i<list.size(); i++) {
      sb.append("$");
      sb.append(list.get(i).length());
      sb.append("\r\n");
      sb.append(list.get(i));
      if(i<end && i<list.size()-1){
        sb.append("\r\n");
      }
    }

    System.out.println("Encoded O/P : " + sb.toString());

    return sb.toString();

  }

  public static String encodeString(String str){
    return "$" + String.valueOf(str.length()) + "\r\n" + str;
  }
}

