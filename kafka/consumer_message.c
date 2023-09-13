#include <consumer_message.h>

consumer_message_t *consumer_message_new(rd_kafka_message_t *rd_kafka_message) {
    if (rd_kafka_message == NULL || rd_kafka_message->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        return NULL;
    }

    size_t size = sizeof(consumer_message_t) + rd_kafka_message->key_len + rd_kafka_message->len;
    consumer_message_t *consumer_message = calloc(size, 1);
    if (consumer_message == NULL) {
        return NULL;
    }

    consumer_message->topic = rd_kafka_topic_name(rd_kafka_message->rkt);
    consumer_message->partition = rd_kafka_message->partition;

    size_t offset = sizeof(consumer_message_t);
    if (rd_kafka_message->key_len > 0) {
        char *key = (char *)consumer_message + offset;
        size_t key_len = rd_kafka_message->key_len;
        memcpy(key, rd_kafka_message->key, key_len);
        offset += key_len;

        consumer_message->key = key;
        consumer_message->key_len = key_len;
    }

    if (rd_kafka_message->len > 0) {
        char *payload = (char *)consumer_message + offset;
        size_t len = rd_kafka_message->len;
        memcpy(payload, rd_kafka_message->payload, len);

        consumer_message->payload = payload;
        consumer_message->len = len;
    }

    consumer_message->offset = rd_kafka_message->offset;

    return consumer_message;
}

static inline consumer_message_t *lua_get_consumer_message(lua_State *L) {
    consumer_message_t **consumer_message_ptr = luaL_checkudata(L, LUA_ARG_SELF, lua_consumer_message_label);
    if (consumer_message_ptr == NULL || *consumer_message_ptr == NULL) {
        luaL_error(L, "invalid consumer_message_t pointer from Lua stack");
        return NULL;
    }

    return *consumer_message_ptr;
}

int lua_consumer_message_topic(lua_State *L) {
    consumer_message_t *consumer_message = lua_get_consumer_message(L);
    lua_pushstring(L, consumer_message->topic);
    return 1;
}

int lua_consumer_message_partition(lua_State *L) {
    consumer_message_t *consumer_message = lua_get_consumer_message(L);
    lua_pushinteger(L, consumer_message->partition);
    return 1;
}

int lua_consumer_message_payload(lua_State *L) {
    consumer_message_t *consumer_message = lua_get_consumer_message(L);
    if (consumer_message->len == 0) {
        return 0;
    }

    lua_pushlstring(L, consumer_message->payload, consumer_message->len);
    return 1;
}

int lua_consumer_message_key(lua_State *L) {
    consumer_message_t *consumer_message = lua_get_consumer_message(L);
    if (consumer_message->key_len == 0) {
        return 0;
    }

    lua_pushlstring(L, consumer_message->key, consumer_message->key_len);
    return 1;
}

int lua_consumer_message_offset(lua_State *L) {
    consumer_message_t *consumer_message = lua_get_consumer_message(L);
    lua_pushinteger(L, consumer_message->offset);
    return 1;
}

int lua_consumer_message_gc(lua_State *L) {
    consumer_message_t **consumer_message_ptr = luaL_checkudata(L, LUA_ARG_SELF, lua_consumer_message_label);
    if (consumer_message_ptr != NULL && *consumer_message_ptr != NULL) {
        consumer_message_t *consumer_message = *consumer_message_ptr;
        free(consumer_message);
        *consumer_message_ptr = NULL;
    }

    return 0;
}
