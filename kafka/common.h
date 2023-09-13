#ifndef COMMON_H
#define COMMON_H

#include <lauxlib.h>
#include <librdkafka/rdkafka.h>
#include <lua.h>
#include <lualib.h>
#include <stdlib.h>
#include <string.h>
#include <tarantool/module.h>

extern const char* const lua_consumer_label;
extern const char* const lua_consumer_message_label;

#define LUA_ARG_SELF 1

#endif  // KAFKA_COMMON_H