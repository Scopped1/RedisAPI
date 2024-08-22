package it.scopped;

import it.scopped.connection.ConnectionManager;
import it.scopped.object.AuthCredentials;
import it.scopped.packet.RedisPacket;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Protocol;

import java.util.Objects;

public class RedisInitializer implements RedisAPI {
    private final ConnectionManager connectionManager;

    private RedisInitializer(Builder builder) {
        final JedisPool jedisPool = this.initPool(builder);
        this.connectionManager = new ConnectionManager(getSingleConnection(builder), jedisPool, builder.channel);
    }

    @Override
    public void sendPacket(RedisPacket packet) {
        this.connectionManager.sendPacket(packet);
    }

    private JedisPool initPool(Builder builder) {
        if (builder.authCredentials == null) {
            return new JedisPool(builder.address, builder.port);
        }

        return new JedisPool(
            builder.address,
            builder.port,
            builder.authCredentials.getUsername(),
            builder.authCredentials.getPassword()
        );
    }

    private Jedis getSingleConnection(Builder builder){
        Jedis jedis = new Jedis(builder.address, builder.port);
        if(builder.authCredentials.getPassword() != null && builder.authCredentials.getUsername() != null){
            jedis.auth(builder.authCredentials.getUsername(), builder.authCredentials.getPassword());
        } else {
            if(builder.authCredentials.getPassword() != null){
                jedis.auth(builder.authCredentials.getPassword());
            }
        }
        return jedis;
    }

    public static class Builder {
        private String address;
        private int port = Protocol.DEFAULT_PORT;
        private AuthCredentials authCredentials;
        private String channel;

        public Builder address(String address) {
            this.address = address;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder withCredentials(AuthCredentials authCredentials) {
            this.authCredentials = authCredentials;
            return this;
        }

        public Builder channel(String channel) {
            this.channel = channel;
            return this;
        }

        public RedisInitializer build() {
            Objects.requireNonNull(this.address, "Address cannot be null!");
            Objects.requireNonNull(this.channel, "Channel cannot be null!");

            return new RedisInitializer(this);
        }
    }
}
