package io.github.pandeyayushk.jobstream.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class RedisConfigTest {

    @Test
    public void createsConfigWithoutPassword(){
        String host="localhost";
        int port=6379;
        RedisConfig config=new RedisConfig(host,port);

        assertEquals(host,config.host());
        assertEquals(port,config.port());
        assertNull(config.password());
    }

    @Test
    public void createsConfigWithPassword(){
        String host="localhost";
        int port=6379;
        String password="Jedis Password";
        RedisConfig config=new RedisConfig(host,port,password);

        assertEquals(host,config.host());
        assertEquals(port,config.port());
        assertEquals(password,config.password());
    }

    @Test
    public void rejectsNullHost(){
        assertThrowsExactly(IllegalArgumentException.class,()->
                new RedisConfig(null,6379)
        );
    }

    @Test
    public void rejectsBlankHost(){
        assertThrowsExactly(IllegalArgumentException.class,()->
                new RedisConfig("   ",6379)
        );
    }

    @Test
    public void rejectsPortBelowValidRange(){
        assertThrowsExactly(IllegalArgumentException.class,()->
                new RedisConfig("localhost",-6379)
        );
    }

    @Test
    public void rejectsPortAboveValidRange(){
        assertThrowsExactly(IllegalArgumentException.class,()->
                new RedisConfig("localhost",66001)
        );
    }

    @Test
    public void acceptsBoundaryPorts(){
        assertDoesNotThrow(() -> {
            new RedisConfig("localhost",1);
        });
        assertDoesNotThrow(() -> {
            new RedisConfig("localhost",65535);
        });
    }
}
