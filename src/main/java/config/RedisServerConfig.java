package config;

public class RedisServerConfig {

    private static int port = 6379;
    private static String dir = null;
    private static String dbfilename = null;

    private static String masterHost = null;
    private static int masterPort = -1;
    private static boolean isMaster = true;

    /// We assume here that caller will always pass argument key and value.
    public RedisServerConfig(String[] args) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--port")) {
                port = Integer.parseInt(args[i+1]);
            } else if (args[i].equals("--dir")) {
                dir = args[i+1];
            } else if (args[i].equals("--dbfilename")) {
                dbfilename = args[i+1];
            } else if (args[i].equals("--replicaof")) {
                String masterHostAndPort = args[i+1];
                masterHost = masterHostAndPort.split(" ")[0];
                masterPort = Integer.parseInt(masterHostAndPort.split(" ")[1]);
                isMaster = false;
            }
        }
        printRedisServerConfig();
    }

    public int getPort() {
        return port;
    }

    public String getDir() {
        return dir;
    }

    public String getDbFilename() {
        return dbfilename;
    }

    public String getMasterHost() {
        return masterHost;
    }

    public int getMasterPort() {
        return masterPort;
    }

    public boolean isMaster() {
        return isMaster;
    }

    public boolean isReplica() {
        return !isMaster;
    }

    public void printRedisServerConfig() {
        String redisServerConfig = "Port: " + port + ", dir: " + dir + ", dbfilename: " + dbfilename +
                ", MasterHost: " + masterHost + ", MasterPort: " + masterPort + ", isMaster: " + isMaster;

        System.out.println(redisServerConfig);
    }
}

