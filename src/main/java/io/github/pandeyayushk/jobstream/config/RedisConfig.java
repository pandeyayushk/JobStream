package io.github.pandeyayushk.jobstream.config;


public class RedisConfig {
    private final String host;
    private final int port;
    private final String password;

    public RedisConfig(String host, int port) {
        this(host,port,null);
    }
    public RedisConfig(String host, int port, String password) {
        if(host==null||host.isBlank()){
            throw new IllegalArgumentException("Host cannot be null or blank");
        }
        if(port<1||port>65535){
            throw new IllegalArgumentException("Port is outside TCP range");
        }
        this.host = host;
        this.port = port;
        this.password = password;
    }

    public int port() {
        return port;
    }

    public String host(){
        return host;
    }

    public String password(){
        return password;
    }
}
