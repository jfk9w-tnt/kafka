include_directories(${RDKAFKA_INCLUDE_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

add_library(kafka SHARED common.c consumer_message.c consumer.c kafka.c)

if (APPLE)
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} \
        -undefined suppress -flat_namespace")
endif(APPLE)

find_package(Threads REQUIRED)
target_link_libraries(kafka ${CMAKE_THREAD_LIBS_INIT})
set_target_properties(kafka PROPERTIES C_VISIBILITY_PRESET hidden)

target_link_libraries(kafka ${RDKAFKA_LIBRARY})
set_target_properties(kafka PROPERTIES PREFIX "" OUTPUT_NAME "kafka")

install(TARGETS kafka LIBRARY DESTINATION ${TARANTOOL_INSTALL_LIBDIR})
