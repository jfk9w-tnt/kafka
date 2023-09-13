#include <kafka.h>

static inline void lua_register_methods(lua_State *L, const char *label, const luaL_Reg methods[]) {
    luaL_newmetatable(L, label);
    lua_pushvalue(L, -1);
    luaL_register(L, NULL, methods);
    lua_setfield(L, -2, "__index");
    lua_pushstring(L, label);
    lua_setfield(L, -2, "__metatable");
    lua_pop(L, 1);
}

LUA_API int __attribute__((visibility("default"))) luaopen_kafka(lua_State *L) {
    static const struct luaL_Reg consumer_methods[] = {
        {"assign", lua_consumer_assign},
        {"poll", lua_consumer_poll},
        {"destroy", lua_consumer_destroy},
    };

    lua_register_methods(L, lua_consumer_label, consumer_methods);

    static const struct luaL_Reg consumer_message_methods[] = {
        {"topic", lua_consumer_message_topic},
        {"partition", lua_consumer_message_partition},
        {"payload", lua_consumer_message_payload},
        {"key", lua_consumer_message_key},
        {"offset", lua_consumer_message_offset},
        {"__gc", lua_consumer_message_gc},
    };

    lua_register_methods(L, lua_consumer_message_label, consumer_message_methods);

    static const struct luaL_Reg meta[] = {
        {"create_consumer", lua_create_consumer},
        {NULL, NULL},
    };

    lua_newtable(L);
    luaL_register(L, NULL, meta);

    return 1;
}
