
#include "kafka.h"
#include "kafka-table.h"

#ifdef __cplusplus
extern "C" {
#endif

    DLLEXP my_bool kafka_disconnect_init(UDF_INIT * initid, UDF_ARGS * args, char *message);
    DLLEXP void kafka_disconnect_deinit(UDF_INIT * initid);
    DLLEXP long long kafka_disconnect(UDF_INIT * initid, UDF_ARGS * args, char *is_null, char *error);

#ifdef __cplusplus
};
#endif

my_bool kafka_disconnect_init(UDF_INIT *UNUSED(initid), UDF_ARGS *args, char *message) {
    debug_print("%s\n", __FUNCTION__);

    if (args->arg_count > 1) {
        strncpy(message, "Wrong arguments. Need: none, or 'topic name' (string)", MYSQL_ERRMSG_SIZE);
        return 1;
    }

    /*
     * Coerced rather than rejected, and deliberately not tested for NULL here:
     * see the note in kafka-cmd-send.c on why args->args[] cannot be checked
     * at init time.
     */
    if (args->arg_count == 1) {
        args->arg_type[0] = STRING_RESULT;
    }

    if (kafka_table_initialized() == 0) {
        strncpy(message, "Kafka brokers has not been initialized.", MYSQL_ERRMSG_SIZE);
        return 1;
    }

    return 0;
}

void kafka_disconnect_deinit(UDF_INIT *UNUSED(initid)) {
    debug_print("%s\n", __FUNCTION__);
}

long long kafka_disconnect(UDF_INIT *UNUSED(initid), UDF_ARGS *args, char *UNUSED(is_null), char *error) {
    char topic[KAFKA_MAX_TOPIC_LEN + 1];
    size_t topiclen;

    debug_print("%s\n", __FUNCTION__);

    /* No argument: tear down every topic and forget the brokers */
    if (args->arg_count == 0) {
        kafka_table_dismiss_all();
        return 0;
    }

    /*
     * One argument: tear down that topic alone, leaving the brokers and every
     * other topic running. Counted, not NUL terminated, as in kafka_send().
     */
    topiclen = args->lengths[0];

    /* A NULL here is a genuine SQL NULL, unlike the same test in _init() */
    if (args->args[0] == NULL) {
        error_print("%s: NULL argument\n", __FUNCTION__);
        *error = 1;
        return -1;
    }

    if (topiclen == 0 || topiclen > KAFKA_MAX_TOPIC_LEN) {
        error_print("%s: Invalid topic name length (%zu)\n", __FUNCTION__, topiclen);
        *error = 1;
        return -1;
    }

    memcpy(topic, args->args[0], topiclen);
    topic[topiclen] = '\0';

    if (kafka_table_dismiss(topic) != 0) {
        *error = 1;
        return -1;
    }

    return 0;
}
