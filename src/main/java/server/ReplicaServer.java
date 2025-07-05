package server;

import config.RedisServerConfig;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.time.Instant;
import java.util.HashMap;

public class ReplicaServer {

    private static RedisServerConfig replicaConfig = null;
    private static Socket masterServerSocket =  null;
    private static OutputStream masterOpStream = null;
    private static InputStream masterIpStream = null;
    byte[] responseBytes = new byte[1024];


    public ReplicaServer(RedisServerConfig config) {
        replicaConfig = config;
    }

    public void start() {
        try {
            masterServerSocket = new Socket(replicaConfig.getMasterHost(), replicaConfig.getMasterPort());
            masterOpStream = masterServerSocket.getOutputStream();
            masterIpStream = masterServerSocket.getInputStream();
            doHandShake();
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    private void doHandShake() throws IOException {
        handShakeStep1();
        handShakeStep2();
        handShakeStep3();


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
}
