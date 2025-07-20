package server;

import config.RedisServerConfig;
import utils.RedisStream;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RedisServer {

    private RedisServerConfig config = null;
    private ServerSocket serverSocket = null;
    private Socket clientSocket = null;
    private List<Socket> replicaSocketList = new ArrayList<>();
    private ConcurrentHashMap<Socket, Integer> replicaSocketToReplConfGetAckCount = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Socket, Integer> replicaSocketToReplOffset = new ConcurrentHashMap<>();
    private int masterOffset = 0;
    private String lastStreamId = null;
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
        HashMap<String, Object> map = new HashMap<>();
        HashMap<String, Long> expiryMap = new HashMap<>();

        try{
            while(true) {
                byte[] input = new byte[1024];
                int port = clientSocket.getPort();
                int bytesRead = clientSocket.getInputStream().read(input);
                if (bytesRead == -1) {
                    System.out.println("Peer closed the connection normally. port: " + port);
                    break;
                }

                String inputString = new String(input).trim();

                if (inputString.length() == 0) {
                    continue;
                }

                System.out.println("Thread: " + Thread.currentThread().getId() + " Client Received: " + inputString);
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
                            } else if (splitedString[i].toLowerCase().equals("ack")) {
                                i = i + 2;
                                int replicaOffset = Integer.parseInt(splitedString[i]);
                                replicaSocketToReplOffset.put(clientSocket, replicaOffset);
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
                            break;
                        case "type":
                            i = i + 2;
                            String typeCommandArg = splitedString[i];
                            if (map.containsKey(typeCommandArg)) {
                                Object val = map.get(typeCommandArg);
                                if (val instanceof String) {
                                    clientSocket.getOutputStream().write("+string\r\n".getBytes());
                                } else if (val instanceof RedisStream) {
                                    clientSocket.getOutputStream().write("+stream\r\n".getBytes());
                                }
                            } else {
                                clientSocket.getOutputStream().write("+none\r\n".getBytes());
                            }
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

                            for (Socket replicaSocket : replicaSocketList) {
                                if (replicaSocket != null) {
                                    String res = inputString + "\r\n";
                                    replicaSocket.getOutputStream().write(res.getBytes());
                                } else {
                                    System.out.println("Replica Socket is null");
                                }
                            }
                            clientSocket.getOutputStream().write("+OK\r\n".getBytes());
                            masterOffset += inputString.getBytes().length;
                            masterOffset += 2;
                            break;
                        case "xadd":
                            i = i + 2;
                            String streamKey = splitedString[i];
                            i = i + 2;
                            String id = splitedString[i];
                            i = i + 2;
                            String k = splitedString[i];
                            i = i + 2;
                            String xaddVal = splitedString[i];

                            // ToDo: Verify if offset should be only maintained for successful ops or all
                            masterOffset += inputString.getBytes().length;
                            masterOffset += 2;
                            String[] currIdArr = id.split("-");
                            String errorMessage = "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
                            if (lastStreamId != null) {
                                String prevStreamId = lastStreamId;
                                String[] prevStreamIdArr = prevStreamId.split("-");
                                if (Long.parseLong(currIdArr[0]) <= 0 && Long.parseLong(currIdArr[1]) <= 0){
                                    clientSocket.getOutputStream().write("-ERR The ID specified in XADD must be greater than 0-0\r\n".getBytes());
                                    break;
                                } else if (Long.parseLong(currIdArr[0]) < Long.parseLong(prevStreamIdArr[0])) {
                                    clientSocket.getOutputStream().write(errorMessage.getBytes());
                                    break;
                                } else if (Long.parseLong(currIdArr[0]) == Long.parseLong(prevStreamIdArr[0]) &&
                                        Long.parseLong(currIdArr[1]) <= Long.parseLong(prevStreamIdArr[1])) {
                                    clientSocket.getOutputStream().write(errorMessage.getBytes());
                                    break;
                                }
                            }
                            if (lastStreamId == null && Long.parseLong(currIdArr[0]) <= 0 && Long.parseLong(currIdArr[1]) <= 0) {
                                errorMessage = "-ERR The ID specified in XADD must be greater than 0-0\r\n";
                                clientSocket.getOutputStream().write(errorMessage.getBytes());
                                break;
                            }
                            RedisStream rd = new RedisStream(id, k, xaddVal);
                            map.put(streamKey, rd);
                            lastStreamId = id;
                            String message = String.format("$%s\r\n%s\r\n", id.length(), id);
                            clientSocket.getOutputStream().write(message.getBytes());
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
                                    int len = 0;
                                    String val = "";
                                    Object getValue = map.get(getKey);
                                    if (getValue instanceof String) {
                                        val = (String) getValue;
                                        len = val.length();
                                    }

                                    String res1 = "$"+len+"\r\n"+val+"\r\n";
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

                        case "wait":
                            Instant currentTimestampWaitCommand = Instant.now();
                            long startEpochMilliWaitCommand = currentTimestampWaitCommand.toEpochMilli();

                            i = i + 2;
                            int expectedReplicaCount = Integer.parseInt(splitedString[i]);
                            i = i + 2;
                            long timeoutMilli = Long.parseLong(splitedString[i]);
                            System.out.println(expectedReplicaCount + " " + timeoutMilli);

                            long timeoutInstance = startEpochMilliWaitCommand + timeoutMilli;
                            Set<Socket> verifiedReplica = new HashSet<>();
                            Set<Socket> contactedReplica = new HashSet<>();
                            while (Instant.now().toEpochMilli() < timeoutInstance && verifiedReplica.size() < expectedReplicaCount) {
                                for (Socket replicaSocket : replicaSocketList) {
                                    if (verifiedReplica.contains(replicaSocket)) {
                                        continue;
                                    }
                                    if (replicaSocket != null) {
                                        if (!contactedReplica.contains(replicaSocket)) {
                                            String replConfGetAck = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
                                            replicaSocket.getOutputStream().write(replConfGetAck.getBytes());
                                            contactedReplica.add(replicaSocket);
                                            replicaSocketToReplConfGetAckCount.put(replicaSocket,
                                                    replicaSocketToReplConfGetAckCount.getOrDefault(replicaSocket, 0) + 1);
                                        }

                                        int replicaOffset = replicaSocketToReplOffset.getOrDefault(replicaSocket, 0);
                                        if (replicaOffset == 0) {
                                            continue;
                                        }
                                        int replicaOffsetExcludingGetAck = replicaOffset - (37 * (replicaSocketToReplConfGetAckCount.get(replicaSocket) - 1));
                                        if (masterOffset == replicaOffsetExcludingGetAck) {
                                            verifiedReplica.add(replicaSocket);
                                        }
                                    }
                                }
                            }

                            if (masterOffset == 0) {
                                String replicaCountResp = ":" + replicaSocketList.size() + "\r\n";
                                clientSocket.getOutputStream().write(replicaCountResp.getBytes());
                            } else {
                                String replicaCountResp = ":" + verifiedReplica.size() + "\r\n";
                                clientSocket.getOutputStream().write(replicaCountResp.getBytes());
                            }
                            break;
                    }
                    i++;
                }
            }
        } catch (SocketException e) {
            System.out.println("Handle Client SocketException: " + e.getMessage());
        } catch (Exception e) {
            System.out.println("Handle Client IOException: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
