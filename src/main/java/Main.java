import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
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
    ExecutorService executorService = Executors.newFixedThreadPool(10);


    try {
      serverSocket = new ServerSocket(port);
      // Since the tester restarts your program quite often, setting SO_REUSEADDR
      // ensures that we don't run into 'Address already in use' errors
      serverSocket.setReuseAddress(true);
      // Wait for connection from client.
      while (true) {
        clientSocket = serverSocket.accept();
        Socket clientCopy = clientSocket;
        executorService.submit(() -> handleClient(clientCopy));
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

  public static void handleClient(Socket clientSocket) {
    HashMap<String, String> map = new HashMap<>();
    try{
      while(true) {
        byte[] input = new byte[1024];
        clientSocket.getInputStream().read(input);
        String inputString = new String(input).trim();

        if (inputString.length() == 0) {
          continue;
        }

        System.out.println("Received: " + inputString);
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
              i = i + 2;
              String key = splitedString[i];
              i = i + 2;
              String value = splitedString[i];
              map.put(key, value);
              clientSocket.getOutputStream().write("+OK\r\n".getBytes());
              break;
            case "get":
              i = i + 2;
              String getKey = splitedString[i];
              if (map.containsKey(getKey)) {
                String getValue = map.get(getKey);
                int len = getValue.length();
                String res = "$"+len+"\r\n"+getValue+"\r\n";
                clientSocket.getOutputStream().write(res.getBytes());
              } else {
                clientSocket.getOutputStream().write("$-1\r\n".getBytes());
              }
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
