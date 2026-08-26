
#include "kafka.h"
#include "kafka-table.h"

#ifdef __cplusplus
extern "C" {
#endif

    DLLEXP my_bool kafka_send_init(UDF_INIT * initid, UDF_ARGS * args, char *message);
    DLLEXP void kafka_send_deinit(UDF_INIT * initid);
    DLLEXP long long kafka_send(UDF_INIT * initid, UDF_ARGS * args, char *is_null, char *error);

#ifdef __cplusplus
};
#endif

my_bool kafka_send_init(UDF_INIT *UNUSED(initid), UDF_ARGS *args, char *message) {
    debug_print("%s\n", __FUNCTION__);

    if (args->arg_count != 2) {
        strncpy(message, "Wrong arguments to function.", MYSQL_ERRMSG_SIZE);
        return 1;
    }

    /*
     * Ask the server to hand both arguments over as strings instead of
     * rejecting anything that is not one already. Assigning to arg_type[] in
     * _init() makes MariaDB coerce the argument for us, so an INT column, a
     * DATE or a DECIMAL can be sent without the caller having to CAST it.
     */
    args->arg_type[0] = STRING_RESULT;
    args->arg_type[1] = STRING_RESULT;

    /*
     * There is deliberately no NULL test on args->args[] here. At init time
     * that pointer is populated only for *constant* arguments; for a column
     * reference or an expression such as JSON_OBJECT(...) it is always NULL.
     * Testing it therefore rejected every non-constant argument, which is
     * precisely the trigger use case this function exists for. A genuine SQL
     * NULL is detected at call time, in kafka_send() below.
     */

    if (kafka_table_connected() == 0) {
        strncpy(message, "Not connected. Call kafka_connect() first.", MYSQL_ERRMSG_SIZE);
        return 1;
    }

    return 0;
}

void kafka_send_deinit(UDF_INIT *UNUSED(initid)) {
    debug_print("%s\n", __FUNCTION__);
}

long long kafka_send(UDF_INIT *UNUSED(initid), UDF_ARGS *args, char *UNUSED(is_null), char *error) {
    /*
     * UDF string arguments are counted, not NUL terminated, so their length
     * must be taken from args->lengths[] and never from strlen(). The payload
     * is passed around with an explicit length and may contain NUL bytes; the
     * topic is used as a hash key, so it needs a terminated copy of its own.
     */
    char topic[KAFKA_MAX_TOPIC_LEN + 1];
    size_t topiclen = args->lengths[0];
    char *msg = args->args[1];
    size_t len = args->lengths[1];
    int sent;

    /* A NULL here is a genuine SQL NULL, unlike the same test in _init() */
    if (args->args[0] == NULL || msg == NULL) {
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

    if (kafka_table_exists(topic) == 0) {
        debug_print("%s: Connection does not exists. Creating connection for topic '%s'\n", __FUNCTION__, topic);

        if (kafka_table_create(topic) != 0) {
            error_print("%s: Cannot initialize new kafka topic\n", __FUNCTION__);
            *error = 1;
            return -1;
        }
    }

    /* Signed: kafka_table_send() reports failure as -1, not as zero */
    sent = kafka_table_send(topic, msg, len);
    if (sent < 0) {
        error_print("%s: Error sending message to kafka topic\n", __FUNCTION__);
        *error = 1;
        return -1;
    }

    return (long long) sent;
}
