package utils;

public class RedisStream {
    private static String id = null;
    private static String key = null;
    private static String value = null;

    public RedisStream(String id, String key, String value) {
        this.id = id;
        this.key = key;
        this.value = value;
    }
}
