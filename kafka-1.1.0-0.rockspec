package = "kafka"
version = "1.1.0-0"
source = {
    url = "git+https://github.com/jfk9w-tnt/kafka.git",
    branch = 'dev',
}
description = {
    summary = "Kafka library for Tarantool",
    homepage = "https://github.com/jfk9w-tnt/kafka",
    license = "Apache",
}
dependencies = {
    "lua >= 5.1" -- actually tarantool > 1.6
}
external_dependencies = {
    TARANTOOL = {
        header = 'tarantool/module.h'
    }
}
build = {
    type = 'cmake';
    variables = {
        CMAKE_BUILD_TYPE="RelWithDebInfo",
        TARANTOOL_DIR="$(TARANTOOL_DIR)",
        TARANTOOL_INSTALL_LIBDIR="$(LIBDIR)",
        TARANTOOL_INSTALL_LUADIR="$(LUADIR)",
        STATIC_BUILD="$(STATIC_BUILD)",
        WITH_OPENSSL_1_1="$(WITH_OPENSSL_1_1)"
    }
}
