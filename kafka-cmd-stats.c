
#include "kafka.h"
#include "kafka-table.h"

#ifdef __cplusplus
extern "C" {
#endif

    DLLEXP my_bool kafka_stats_init(UDF_INIT * initid, UDF_ARGS * args, char *message);
    DLLEXP void kafka_stats_deinit(UDF_INIT * initid);
    DLLEXP char *kafka_stats(UDF_INIT * initid, UDF_ARGS * args, char *result, unsigned long *length, char *is_null, char *error);

#ifdef __cplusplus
};
#endif

my_bool kafka_stats_init(UDF_INIT *UNUSED(initid), UDF_ARGS *args, char *message) {
    debug_print("%s\n", __FUNCTION__);

    if (args->arg_count != 1) {
        strncpy(message, "Wrong arguments. Need: 'topic name' (string)", MYSQL_ERRMSG_SIZE);
        return 1;
    }

    if (args->arg_type[0] != STRING_RESULT) {
        strncpy(message, "Wrong arguments. Need: 'topic' (string)", MYSQL_ERRMSG_SIZE);
        return 1;
    }

    if (kafka_table_initialized() == 0) {
        strncpy(message, "Kafka has not been initialized.", MYSQL_ERRMSG_SIZE);
        return 1;
    }

    /*
     * args->args[0] is populated here only for a *constant* argument (see the
     * note in kafka-cmd-send.c), so an unused topic can only be rejected up
     * front when the caller wrote a literal. For a column or an expression the
     * lookup has to wait until call time, where an unknown topic simply reports
     * zero counters.
     */
    if (args->args[0] != NULL) {
        char topic[KAFKA_MAX_TOPIC_LEN + 1];
        size_t topiclen = args->lengths[0];

        if (topiclen == 0 || topiclen > KAFKA_MAX_TOPIC_LEN) {
            strncpy(message, "Wrong arguments. Invalid topic name length.", MYSQL_ERRMSG_SIZE);
            return 1;
        }

        memcpy(topic, args->args[0], topiclen);
        topic[topiclen] = '\0';

        if (kafka_table_exists(topic) == 0) {
            strncpy(message, "Cannot show stats for an unused topic", MYSQL_ERRMSG_SIZE);
            return 1;
        }
    }

    return 0;
}

void kafka_stats_deinit(UDF_INIT *UNUSED(initid)) {
    debug_print("%s\n", __FUNCTION__);
}

char *kafka_stats(UDF_INIT *UNUSED(initid), UDF_ARGS *args, char *result, unsigned long *length, char *UNUSED(is_null), char *UNUSED(error)) {
    uint32_t transferred, failed;
    int written;
    char topic[KAFKA_MAX_TOPIC_LEN + 1];
    size_t topiclen = args->lengths[0];

    debug_print("%s\n", __FUNCTION__);

    /* Counted, not NUL terminated: see the comment in kafka-cmd-send.c */
    if (args->args[0] == NULL || topiclen == 0 || topiclen > KAFKA_MAX_TOPIC_LEN) {
        topiclen = 0;
    } else {
        memcpy(topic, args->args[0], topiclen);
    }
    topic[topiclen] = '\0';

    kafka_topic_get_stats(topic, &transferred, &failed);

    /* Short enough to always fit the buffer the server supplied */
    written = snprintf(result, KAFKA_UDF_RESULT_LEN, "Transferred: %u Failed: %u", transferred, failed);

    /* snprintf() reports what it would have written, so clamp to what it did */
    if (written < 0) {
        written = 0;
    } else if (written >= KAFKA_UDF_RESULT_LEN) {
        written = KAFKA_UDF_RESULT_LEN - 1;
    }

    *length = (unsigned long) written;

    return result;
}
