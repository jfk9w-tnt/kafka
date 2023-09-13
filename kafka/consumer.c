#include <consumer.h>

int lua_create_consumer(lua_State *L) {
    if (lua_gettop(L) != 1 || !lua_istable(L, LUA_CREATE_CONSUMER_ARG_CONF)) {
        return luaL_error(L, LUA_CREATE_CONSUMER_USAGE);
    }

    lua_pushstring(L, "brokers");
    lua_gettable(L, LUA_CREATE_CONSUMER_ARG_CONF);
    const char *brokers = lua_tostring(L, -1);
    lua_pop(L, 1);
    if (brokers == NULL) {
        return luaL_error(L, "brokers must be a string");
    }

    char errstr[512];

    rd_kafka_conf_t *rd_kafka_conf = rd_kafka_conf_new();
    if (rd_kafka_conf == NULL) {
        return luaL_error(L, "out of memory: failed to allocate rd_kafka_conf_t");
    }

    lua_pushstring(L, "options");
    lua_gettable(L, LUA_CREATE_CONSUMER_ARG_CONF);
    if (lua_istable(L, -1)) {
        lua_pushnil(L);
        while (lua_next(L, -2)) {
            const char *key = lua_tostring(L, -2);
            const char *value = lua_tostring(L, -1);
            if (key == NULL || value == NULL) {
                rd_kafka_conf_destroy(rd_kafka_conf);
                return luaL_error(L, "consumer config options must contain string keys and values");
            }

            if (rd_kafka_conf_set(rd_kafka_conf, key, value, errstr, sizeof(errstr))) {
                rd_kafka_conf_destroy(rd_kafka_conf);
                lua_pushnil(L);
                lua_pushstring(L, errstr);
                return 2;
            }

            lua_pop(L, 1);
        }
    }

    lua_pop(L, 1);

    rd_kafka_t *rd_kafka = rd_kafka_new(RD_KAFKA_CONSUMER, rd_kafka_conf, errstr, sizeof(errstr));
    if (rd_kafka == NULL) {
        rd_kafka_conf_destroy(rd_kafka_conf);
        lua_pushnil(L);
        lua_pushstring(L, errstr);
        return 2;
    }

    if (rd_kafka_brokers_add(rd_kafka, brokers) == 0) {
        rd_kafka_destroy(rd_kafka);
        return luaL_error(L, "no valid brokers found");
    }

    rd_kafka_resp_err_t rd_kafka_resp_err = rd_kafka_poll_set_consumer(rd_kafka);
    if (rd_kafka_resp_err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        rd_kafka_destroy(rd_kafka);
        lua_pushnil(L);
        lua_pushstring(L, rd_kafka_err2str(rd_kafka_resp_err));
        return 2;
    }

    consumer_t *consumer = malloc(sizeof(consumer_t));
    if (consumer == NULL) {
        rd_kafka_destroy(rd_kafka);
        return luaL_error(L, "out of memory: failed to allocate consumer_t");
    }

    consumer->rd_kafka = rd_kafka;

    consumer_t **consumer_ptr = lua_newuserdata(L, sizeof(consumer));
    *consumer_ptr = consumer;

    luaL_getmetatable(L, lua_consumer_label);
    lua_setmetatable(L, -2);

    return 1;
}

static inline consumer_t *lua_get_consumer(lua_State *L) {
    consumer_t **consumer_ptr = luaL_checkudata(L, LUA_ARG_SELF, lua_consumer_label);
    if (consumer_ptr == NULL || *consumer_ptr == NULL) {
        luaL_error(L, "invalid consumer_t pointer from Lua stack");
        return NULL;
    }

    return *consumer_ptr;
}

static inline rd_kafka_topic_partition_list_t *
lua_get_rd_kafka_topic_partition_list(lua_State *L, int index, const char *usage) {
    rd_kafka_topic_partition_list_t *rd_kafka_topic_partition_list = NULL;
    if (lua_isnil(L, index)) {
        return rd_kafka_topic_partition_list;
    }

    if (!lua_istable(L, index)) {
        luaL_error(L, usage);
        return NULL;
    }

    size_t objlen = lua_objlen(L, index);
    rd_kafka_topic_partition_list = rd_kafka_topic_partition_list_new(objlen);
    if (rd_kafka_topic_partition_list == NULL) {
        luaL_error(L, "out of memory: failed to allocate rd_kafka_topic_partition_list_t");
        return NULL;
    }

    for (size_t i = 1; i <= objlen; i++) {
        luaL_pushint64(L, i);
        lua_gettable(L, index);

        if (!lua_istable(L, -1)) {
            rd_kafka_topic_partition_list_destroy(rd_kafka_topic_partition_list);
            luaL_error(L, usage);
            return NULL;
        }

        luaL_pushint64(L, 1);
        lua_gettable(L, -2);
        if (!lua_isstring(L, -1)) {
            rd_kafka_topic_partition_list_destroy(rd_kafka_topic_partition_list);
            luaL_error(L, usage);
            return NULL;
        }

        const char *topic = lua_tostring(L, -1);
        lua_pop(L, 1);

        luaL_pushint64(L, 2);
        lua_gettable(L, -2);
        if (!lua_isnumber(L, -1)) {
            rd_kafka_topic_partition_list_destroy(rd_kafka_topic_partition_list);
            luaL_error(L, usage);
            return NULL;
        }

        int32_t partition = lua_tointeger(L, -1);
        lua_pop(L, 1);

        rd_kafka_topic_partition_t *rd_kafka_topic_partition =
            rd_kafka_topic_partition_list_add(rd_kafka_topic_partition_list, topic, partition);

        if (lua_objlen(L, -1) == 3) {
            luaL_pushint64(L, 3);
            lua_gettable(L, -2);
            if (!lua_isnumber(L, -1)) {
                rd_kafka_topic_partition_list_destroy(rd_kafka_topic_partition_list);
                luaL_error(L, usage);
                return NULL;
            }

            int64_t offset = luaL_toint64(L, -1);
            lua_pop(L, 1);

            rd_kafka_topic_partition->offset = offset;
        }

        lua_pop(L, 1);
    }

    return rd_kafka_topic_partition_list;
}

int lua_consumer_assign(lua_State *L) {
    if (lua_gettop(L) != 2) {
        return luaL_error(L, LUA_CONSUMER_ASSIGN_USAGE);
    }

    consumer_t *consumer = lua_get_consumer(L);
    rd_kafka_topic_partition_list_t *rd_kafka_topic_partition_list =
        lua_get_rd_kafka_topic_partition_list(L, 2, LUA_CONSUMER_ASSIGN_USAGE);

    rd_kafka_resp_err_t rd_kafka_resp_err = rd_kafka_assign(consumer->rd_kafka, rd_kafka_topic_partition_list);
    if (rd_kafka_resp_err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        rd_kafka_topic_partition_list_destroy(rd_kafka_topic_partition_list);
        lua_pushstring(L, rd_kafka_err2str(rd_kafka_resp_err));
        return 1;
    }

    return 0;
}

int lua_consumer_poll(lua_State *L) {
    if (lua_gettop(L) != 2 || !lua_isnumber(L, LUA_CONSUMER_POLL_ARG_TIMEOUT)) {
        return luaL_error(L, LUA_CONSUMER_POLL_USAGE);
    }

    consumer_t *consumer = lua_get_consumer(L);
    int timeout_ms = lua_tointeger(L, LUA_CONSUMER_POLL_ARG_TIMEOUT);

    rd_kafka_message_t *rd_kafka_message = rd_kafka_consumer_poll(consumer->rd_kafka, timeout_ms);
    if (rd_kafka_message == NULL) {
        return 0;
    }

    rd_kafka_resp_err_t rd_kafka_resp_err = rd_kafka_message->err;
    if (rd_kafka_resp_err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        rd_kafka_message_destroy(rd_kafka_message);
        lua_pushnil(L);
        lua_pushstring(L, rd_kafka_err2str(rd_kafka_resp_err));
        return 2;
    }

    consumer_message_t *consumer_message = consumer_message_new(rd_kafka_message);
    rd_kafka_message_destroy(rd_kafka_message);
    if (consumer_message == NULL) {
        lua_pushnil(L);
        lua_pushliteral(L, "failed to convert rd_kafka_message_t to consumer_message_t");
        return 2;
    }

    consumer_message_t **consumer_message_ptr = lua_newuserdata(L, sizeof(consumer_message));
    *consumer_message_ptr = consumer_message;

    luaL_getmetatable(L, lua_consumer_message_label);
    lua_setmetatable(L, -2);

    return 1;
}

int lua_consumer_destroy(lua_State *L) {
    consumer_t **consumer_ptr = luaL_checkudata(L, LUA_ARG_SELF, lua_consumer_label);
    if (consumer_ptr != NULL) {
        consumer_t *consumer = *consumer_ptr;
        if (consumer != NULL) {
            if (consumer->rd_kafka != NULL) {
                rd_kafka_destroy(consumer->rd_kafka);
                consumer->rd_kafka = NULL;
            }

            free(consumer);
            *consumer_ptr = NULL;
        }
    }

    return 0;
}
