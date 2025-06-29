import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Instant;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");

    ServerSocket serverSocket = null;
    Socket clientSocket = null;
    int port = 6379;
    String masterHost = "";
    int masterPort = 0;
    ExecutorService executorService = Executors.newFixedThreadPool(10);

    try {
      for (int i = 0; i < args.length; i++) {
        if (args[i].equals("--port")) {
          port = Integer.parseInt(args[i+1]);
          System.out.println("port: " + port);
        } else if (args[i].equals("--replicaof")) {
          String masterHostAndPort = args[i+1];
          masterHost = masterHostAndPort.split(" ")[0];
          masterPort = Integer.parseInt(masterHostAndPort.split(" ")[1]);
          Socket masterServerSocket = new Socket(masterHost, masterPort);
          masterServerSocket.getOutputStream().write("*1\r\n$4\r\nPING\r\n".getBytes());

          byte[] input = new byte[1024];
          masterServerSocket.getInputStream().read(input);
          String inputString = new String(input).trim();
          System.out.println("Received: " + inputString);
        }
      }

      serverSocket = new ServerSocket(port);
      // Since the tester restarts your program quite often, setting SO_REUSEADDR
      // ensures that we don't run into 'Address already in use' errors
      serverSocket.setReuseAddress(true);
      // Wait for connection from client.
      while (true) {
        clientSocket = serverSocket.accept();
        Socket clientCopy = clientSocket;
        executorService.submit(() -> handleClient(clientCopy, args));
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

  public static void handleClient(Socket clientSocket, String[] args) {
    HashMap<String, String> map = new HashMap<>();
    HashMap<String, Long> expiryMap = new HashMap<>();

    String dir = "";
    String dbfilename = "";
    boolean isMaster = true;

    for (int i = 0; i<args.length; i++) {
      if (args[i].equals("--dir") && i+1 < args.length) {
        dir = args[i+1];
      } else if (args[i].equals("--dbfilename") && i+1 < args.length) {
        dbfilename = args[i+1];
      } else if (args[i].equals("--replicaof")) {
        isMaster=false;
      }
    }

    try{
      while(true) {
        byte[] input = new byte[1024];
        clientSocket.getInputStream().read(input);
        String inputString = new String(input).trim();

        if (inputString.length() == 0) {
          continue;
        }

        System.out.println("Client Received: " + inputString);
        String[] splitedString = inputString.split("\r\n");

        for (int i = 2; i < splitedString.length; i++) {
          switch (splitedString[i].toLowerCase()) {
            case "echo":
              i = i + 2;
              String opString = "+"+splitedString[i]+"\r\n";
              clientSocket.getOutputStream().write(opString.getBytes());
              break;
            case "ping":
              clientSocket.getOutputStream().write("+PONG\r\n".getBytes());
              break;
            case "set":

              Instant currentTimestamp = Instant.now();
              long epochMilli = currentTimestamp.toEpochMilli();

              i = i + 2;
              String key = splitedString[i];
              i = i + 2;
              String value = splitedString[i];
              map.put(key, value);

              if ( (i+2) < splitedString.length && splitedString[i + 2].toLowerCase().equals("px")) {
                i = i+4;
                long expiry =  Long.parseLong(splitedString[i]);
                expiryMap.put(key, epochMilli + expiry);
              }
              clientSocket.getOutputStream().write("+OK\r\n".getBytes());
              break;
            case "get":
              i = i + 2;
              String getKey = splitedString[i];
              if (map.containsKey(getKey)) {

                Instant currentGetTimestamp = Instant.now();
                long getEpochMilli = currentGetTimestamp.toEpochMilli();

                if (expiryMap.containsKey(getKey) && expiryMap.get(getKey) < getEpochMilli) {
                  map.remove(getKey);
                  expiryMap.remove(getKey);
                  clientSocket.getOutputStream().write("$-1\r\n".getBytes());
                } else {
                  String getValue = map.get(getKey);
                  int len = getValue.length();
                  String res = "$"+len+"\r\n"+getValue+"\r\n";
                  clientSocket.getOutputStream().write(res.getBytes());
                }
              } else {
                clientSocket.getOutputStream().write("$-1\r\n".getBytes());
              }
              break;
            case "config":
              if (i+2 < splitedString.length && splitedString[i+2].toLowerCase().equals("get")) {
                i = i + 4;
                String configGetKey = splitedString[i];
                String respString = "";
                if (configGetKey.equals("dir")) {
                  respString = "*2\r\n$3\r\ndir\r\n$"+dir.length()+"\r\n"+dir+"\r\n";

                } else if (configGetKey.equals("dbfilename")) {
                  respString = "*2\r\n$3\r\ndbfilename\r\n$"+dbfilename.length()+"\r\n"+dbfilename+"\r\n";

                }
                clientSocket.getOutputStream().write(respString.getBytes());
              }
              break;
            case "info":
              String infoReplicationResp = "";
              if (i+2 < splitedString.length && splitedString[i+2].toLowerCase().equals("replication")) {
                // info command called with replication. Respond with only replication info
                if (isMaster) {
                  // The length in Bulk_String such as below is total length of all characters between first and last \r\n
                  // In the case below, total length is 89 = 85 for the string + 4 for the escape characters
                  infoReplicationResp = "$89\r\nrole:master\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0\r\n";
                } else {
                  infoReplicationResp = "$88\r\nrole:slave\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0\r\n";
                }
                clientSocket.getOutputStream().write(infoReplicationResp.getBytes());
              }
              // ToDo: Add support for info command called without replication. Respond with all info
              break;
          }
          i++;
        }
      }
    } catch (Exception e) {
      System.out.println("IOException: " + e.getMessage());
      e.printStackTrace();
    }
  }

}
