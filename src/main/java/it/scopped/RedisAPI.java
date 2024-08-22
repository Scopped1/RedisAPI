package it.scopped;

import it.scopped.packet.RedisPacket;

public interface RedisAPI {

    void sendPacket(RedisPacket packet);

    static RedisInitializer.Builder builder() {
        return new RedisInitializer.Builder();
    }
}