package it.scopped.connection;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import it.scopped.packet.RedisPacket;
import lombok.SneakyThrows;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public class ConnectionManager {
    private final JedisPool jedisPool;
    private final String channel;
    private final ExecutorService executorService = Executors.newFixedThreadPool(2);

    private static final Gson GSON = new GsonBuilder().create();
    private static final String CLASS_IDENTIFIER = "redisapi-packet-class";
    private final Jedis pubConnection;

    public ConnectionManager(Jedis singleConn, JedisPool jedisPool, String channel) {
        this.pubConnection = singleConn;
        this.jedisPool = jedisPool;
        this.channel = channel;
        this.initSubscription();
    }

    @SneakyThrows
    public void sendPacket(RedisPacket packet) {
        final Map<String, Object> fieldMap = new HashMap<>();
        for (final Field field : packet.getClass().getDeclaredFields()) {
            field.setAccessible(true);
            fieldMap.put(field.getName(), field.get(packet));
        }

        final JsonObject jsonObject = GSON.fromJson(GSON.toJson(fieldMap), JsonObject.class);
        jsonObject.addProperty(CLASS_IDENTIFIER, packet.getClass().getName());

        this.executorService.submit(() -> this.handle((jedis) -> jedis.publish(this.channel, jsonObject.toString())));
    }

    private void handle(Consumer<Jedis> consumer) {
        try (final Jedis jedis = this.jedisPool.getResource()) {
            if (jedis == null) {
                throw new IllegalStateException("Failed to handle Jedis event");
            }
            consumer.accept(jedis);
        }
    }

    private void initSubscription() {
        // Handle messages on a separate messaging thread
        new Thread(() -> {
            final JedisPubSub jedisPubSub = new JedisPubSub() {
                @Override
                @SneakyThrows
                public void onMessage(String channel, String message) {
                    if (!channel.equals(ConnectionManager.this.channel)) {
                        return;
                    }

                    final JsonObject jsonObject = GSON.fromJson(message, JsonObject.class);
                    final Class<?> packetClass = Class.forName(jsonObject.remove(CLASS_IDENTIFIER).getAsString());
                    final RedisPacket packet = (RedisPacket) GSON.fromJson(jsonObject, packetClass);

                    packet.onReceive();
                }
            };
            pubConnection.getClient().setTimeoutInfinite();
            pubConnection.subscribe(jedisPubSub, this.channel);
        }).start();
    }

}
