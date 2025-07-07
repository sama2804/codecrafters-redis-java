package server;

import config.RedisServerConfig;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Instant;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ReplicaServer {

    private static RedisServerConfig replicaConfig = null;
    private static Socket masterServerSocket =  null;
    private static OutputStream masterOpStream = null;
    private static InputStream masterIpStream = null;
    byte[] responseBytes = new byte[1024];
    private static Socket clientSocket = null;
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    private HashMap<String, String> map = new HashMap<>();
    private HashMap<String, Long> expiryMap = new HashMap<>();


    public ReplicaServer(RedisServerConfig config) {
        replicaConfig = config;
    }

    public void start() {
        try {

            masterServerSocket = new Socket(replicaConfig.getMasterHost(), replicaConfig.getMasterPort());
            masterOpStream = masterServerSocket.getOutputStream();
            masterIpStream = masterServerSocket.getInputStream();

            ServerSocket replicaServerSocket = new ServerSocket(replicaConfig.getPort());
            replicaServerSocket.setReuseAddress(true);

            doHandShake();

            while (true) {
                clientSocket = replicaServerSocket.accept();
                Socket clientCopy = clientSocket;
                executorService.submit(() -> handleRequest(clientCopy));
            }

        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    private void doHandShake() throws IOException {
        handShakeStep1();
        handShakeStep2();
        handShakeStep3();
        loadRDBFile();
    }

    private void handShakeStep1() throws IOException {
        masterOpStream.write("*1\r\n$4\r\nPING\r\n".getBytes());
        masterOpStream.flush();

        String masterResponse = getResponseFromMaster();
        if (!verifyResponseFromMaster(masterResponse, "PONG")) {
            throw new IOException("HandShake Step 1 verification failed");
        }
    }

    private void handShakeStep2() throws IOException {
        String replConfOutput = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n"+ replicaConfig.getPort() +"\r\n";
        masterOpStream.write(replConfOutput.getBytes());
        masterOpStream.flush();

        String replConfCapaPSync2 = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
        masterOpStream.write(replConfCapaPSync2.getBytes());
        masterOpStream.flush();

        String masterResponse = getResponseFromMaster();
        if (!verifyResponseFromMaster(masterResponse, "OK")) {
            throw new IOException("HandShake Step 2 verification failed");
        }
    }

    private void handShakeStep3() throws IOException {
        String resp = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
        masterOpStream.write(resp.getBytes());
        masterOpStream.flush();
    }

    private void loadRDBFile() throws IOException {
        // ToDo: Handle RDB file returned from master
        System.out.println("In LoadRDB File");
        responseBytes = new byte[1024];
        BufferedReader reader = new BufferedReader(new InputStreamReader(masterIpStream));
        String content = reader.readLine();
        System.out.println("request_PSYNC->" + content);

        reader.readLine();

        char[] buffer = new char[88];
        int read = 0;

        while (read < 88) {
            int r = reader.read(buffer, read, 88 - read);
            if (r == -1) throw new EOFException("Unexpected end of stream");
            read += r;
        }

        new Thread(() -> {
            try {
                processPropagatedCommands(reader);
            } catch (Exception e) {
                System.out.println("Exception: " + e.getMessage());
            }
        }).start();

    }


    private void processPropagatedCommands(BufferedReader reader) throws IOException {
        String line;
        while ((line = reader.readLine()) != null) {
            System.out.println("Received: " + line);

            switch (line.toLowerCase()) {
                case "replconf":
                    reader.readLine();
                    if (reader.readLine().toLowerCase().equals("getack")) {
                        reader.readLine();
                        if (reader.readLine().toLowerCase().equals("*")) {
                            masterOpStream.write("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n".getBytes());
                            masterOpStream.flush();
                        }
                    }
                    break;
                case "set":
//                    Instant currentTimestamp = Instant.now();
//                    long epochMilli = currentTimestamp.toEpochMilli();

                    reader.readLine();
                    String key = reader.readLine();

                    reader.readLine();
                    String value = reader.readLine();

                    map.put(key, value);
//                    if ( (i+2) < splitedString.length && splitedString[i + 2].toLowerCase().equals("px")) {
//                                i = i+4;
//                                long expiry =  Long.parseLong(splitedString[i]);
//                                expiryMap.put(key, epochMilli + expiry);
//                            }

                    break;
                default:
                    System.out.println("In Default while processing propagated commands. line: " + line);
            }
        }
    }

    private String getResponseFromMaster() throws IOException {
        responseBytes = new byte[1024];
        masterIpStream.read(responseBytes);
        String masterResponse = new String(responseBytes).trim();
        System.out.println("Master Response: " + masterResponse);
        return masterResponse;
    }

    private boolean verifyResponseFromMaster(String masterResponse, String expectedResp) {
        if (masterResponse.length() != 0) {
            String[] splitedString = masterResponse.split("\r\n");
            return splitedString[0].equals("+" + expectedResp);
        }
        return false;
    }

    private void handleRequest(Socket clientSocket) {
        try{
            while(true) {

                byte[] input = new byte[1024];
                clientSocket.getInputStream().read(input);
                String inputString = new String(input).trim();

                if (inputString.length() == 0) {
                    continue;
                }
                System.out.println("Replica Received: " + inputString);
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
                                    respString = "*2\r\n$3\r\ndir\r\n$"+replicaConfig.getDir().length()+"\r\n"+replicaConfig.getDir()+"\r\n";

                                } else if (configGetKey.equals("dbfilename")) {
                                    respString = "*2\r\n$3\r\ndbfilename\r\n$"+replicaConfig.getDbFilename().length()+"\r\n"+replicaConfig.getDbFilename()+"\r\n";

                                }
                                clientSocket.getOutputStream().write(respString.getBytes());
                            }
                            break;
                        case "info":
                            String infoReplicationResp = "";
                            if (i+2 < splitedString.length && splitedString[i+2].toLowerCase().equals("replication")) {
                                // info command called with replication. Respond with only replication info
                                if (replicaConfig.isMaster()) {
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
                        default:
                            System.out.println("In Default while processing handleRequest. splitedString[i]: " + splitedString[i]);
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
