#ifndef KAFKA_CONSUMER_H
#define KAFKA_CONSUMER_H

#include <common.h>
#include <consumer_message.h>

typedef struct {
    rd_kafka_t *rd_kafka;
} consumer_t;

#define LUA_CREATE_CONSUMER_USAGE "usage: consumer, err = create_consumer(conf)"
#define LUA_CREATE_CONSUMER_ARG_CONF 1
int lua_create_consumer(lua_State *L);

#define LUA_CONSUMER_ASSIGN_USAGE "usage: err = consumer:assign(x), where x one of: {{topic, partition, offset}}, {{topic, partition}}, or nil"
#define LUA_CONSUMER_ASSIGN_ARG_TP LUA_ARG_SELF + 1
int lua_consumer_assign(lua_State *L);

#define LUA_CONSUMER_POLL_USAGE "usage: msg = consumer:poll(timeout_ms)"
#define LUA_CONSUMER_POLL_ARG_TIMEOUT LUA_ARG_SELF + 1
int lua_consumer_poll(lua_State *L);

int lua_consumer_destroy(lua_State *L);

#endif  // KAFKA_CONSUMER_H