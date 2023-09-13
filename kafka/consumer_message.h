#ifndef CONSUMER_MESSAGE_H
#define CONSUMER_MESSAGE_H

#include <common.h>

typedef struct {
    const char *topic;
    int32_t partition;
    char *payload;
    size_t len;
    char *key;
    size_t key_len;
    int64_t offset;
} consumer_message_t;

consumer_message_t *consumer_message_new(rd_kafka_message_t *rd_kafka_message);

int lua_consumer_message_topic(lua_State *L);
int lua_consumer_message_partition(lua_State *L);
int lua_consumer_message_payload(lua_State *L);
int lua_consumer_message_key(lua_State *L);
int lua_consumer_message_offset(lua_State *L);
int lua_consumer_message_gc(lua_State *L);

#endif  // KAFKA_CONSUMER_MESSAGE_H