package server;

import config.RedisServerConfig;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RedisServer {

    private RedisServerConfig config = null;
    private ServerSocket serverSocket = null;
    private Socket clientSocket = null;
    private List<Socket> replicaSocketList = new ArrayList<>();

    ExecutorService executorService = Executors.newFixedThreadPool(10);

    public RedisServer(RedisServerConfig redisServerConfig) {
        config = redisServerConfig;
    }

    public void start() {
        try {
            if (config.isReplica()) {
                ReplicaServer replicaServer = new ReplicaServer(config);
                replicaServer.start();
            } else {
                serverSocket = new ServerSocket(config.getPort());
                serverSocket.setReuseAddress(true);
                while (true) {
                    clientSocket = serverSocket.accept();
                    Socket clientCopy = clientSocket;
                    executorService.submit(() -> handleClient(clientCopy));
                }
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

    private void handleClient(Socket clientSocket) {
        HashMap<String, String> map = new HashMap<>();
        HashMap<String, Long> expiryMap = new HashMap<>();

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
                        case "replconf":
                            i = i + 2;
                            if (splitedString[i].equals("listening-port")) {
                                replicaSocketList.add(clientSocket);
                                clientSocket.getOutputStream().write("+OK\r\n".getBytes());
                            } else if (splitedString[i].equals("capa")) {
                                i = i + 2;
                                if (splitedString[i].equals("psync2")) {
                                    clientSocket.getOutputStream().write("+OK\r\n".getBytes());
                                }
                            }
                            break;
                        case "psync":
                            i = i + 2;
                            if (splitedString[i].equals("?") && splitedString[i+2].equals("-1")) {
                                clientSocket.getOutputStream().write("+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n".getBytes());
                                clientSocket.getOutputStream().flush();

                                byte[] contents = Base64.getDecoder()
                                        .decode("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==");
                                clientSocket.getOutputStream().write(("$" + contents.length + "\r\n").getBytes());
                                clientSocket.getOutputStream().write(contents);
                            }
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

                            for (Socket replicaSocket : replicaSocketList) {
                                if (replicaSocket != null) {
                                    String res = inputString + "\r\n";
                                    replicaSocket.getOutputStream().write(res.getBytes());
                                } else {
                                    System.out.println("Replica Socket is null");
                                }
                            }
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
                                    String res1 = "$"+len+"\r\n"+getValue+"\r\n";
                                    clientSocket.getOutputStream().write(res1.getBytes());
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
                                    respString = "*2\r\n$3\r\ndir\r\n$"+config.getDir().length()+"\r\n"+config.getDir()+"\r\n";

                                } else if (configGetKey.equals("dbfilename")) {
                                    respString = "*2\r\n$3\r\ndbfilename\r\n$"+config.getDbFilename().length()+"\r\n"+config.getDbFilename()+"\r\n";

                                }
                                clientSocket.getOutputStream().write(respString.getBytes());
                            }
                            break;
                        case "info":
                            String infoReplicationResp = "";
                            if (i+2 < splitedString.length && splitedString[i+2].toLowerCase().equals("replication")) {
                                // info command called with replication. Respond with only replication info
                                if (config.isMaster()) {
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
