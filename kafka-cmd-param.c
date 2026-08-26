
#include "kafka.h"
#include "kafka-table.h"

#ifdef __cplusplus
extern "C" {
#endif

    DLLEXP my_bool kafka_connection_param_init(UDF_INIT * initid, UDF_ARGS * args, char *message);
    DLLEXP void kafka_connection_param_deinit(UDF_INIT * initid);
    DLLEXP long long kafka_connection_param(UDF_INIT * initid, UDF_ARGS * args, char *is_null, char *error);

#ifdef __cplusplus
};
#endif

/*
 * Sets one librdkafka configuration property, to be applied when
 * kafka_connect() builds the producer. Anything librdkafka accepts works here:
 * security.protocol, sasl.mechanism, sasl.username, sasl.password, acks,
 * compression.type, enable.idempotence, the queue and batch sizes, the TLS
 * certificate paths, and so on.
 *
 * The name is not checked against a list of our own. librdkafka validates it,
 * which means this keeps working when librdkafka gains a property and gives
 * librdkafka's own wording when one is wrong.
 */
my_bool kafka_connection_param_init(UDF_INIT *UNUSED(initid), UDF_ARGS *args, char *message) {
    debug_print("%s\n", __FUNCTION__);

    if (args->arg_count != 2) {
        strncpy(message, "Wrong arguments. Need: 'property' (string), 'value' (string)", MYSQL_ERRMSG_SIZE);
        return 1;
    }

    /*
     * Coerced rather than rejected, and deliberately not tested for NULL here:
     * see the note in kafka-cmd-send.c on why args->args[] cannot be checked
     * at init time.
     */
    args->arg_type[0] = STRING_RESULT;
    args->arg_type[1] = STRING_RESULT;

    if (kafka_table_connected()) {
        strncpy(message, "Already connected. Parameters can only be set while disconnected.", MYSQL_ERRMSG_SIZE);
        return 1;
    }

    /*
     * args->args[] is populated here only for constant arguments, which is the
     * normal way this function is called. When it is, the property can be
     * checked now and a bad one refused with librdkafka's own wording -- the
     * main function cannot do that, since its char *error is a single byte and
     * carries no message. A non-constant argument is checked at call time
     * instead.
     */
    if (args->args[0] != NULL && args->args[1] != NULL) {
        char name[KAFKA_MAX_SETUP_VAR_LEN + 1];
        char value[KAFKA_MAX_SETUP_VAR_LEN + 1];
        char errbuf[KAFKA_ERRBUF_LEN];

        if (args->lengths[0] == 0 || args->lengths[0] > KAFKA_MAX_SETUP_VAR_LEN || args->lengths[1] > KAFKA_MAX_SETUP_VAR_LEN) {
            strncpy(message, "Wrong arguments. Property name or value is empty or too long.", MYSQL_ERRMSG_SIZE);
            return 1;
        }

        memcpy(name, args->args[0], args->lengths[0]);
        name[args->lengths[0]] = '\0';

        memcpy(value, args->args[1], args->lengths[1]);
        value[args->lengths[1]] = '\0';

        if (kafka_table_validate_param(name, value, errbuf, sizeof(errbuf)) != 0) {
            strncpy(message, errbuf, MYSQL_ERRMSG_SIZE);
            return 1;
        }
    }

    return 0;
}

void kafka_connection_param_deinit(UDF_INIT *UNUSED(initid)) {
    debug_print("%s\n", __FUNCTION__);
}

long long kafka_connection_param(UDF_INIT *UNUSED(initid), UDF_ARGS *args, char *UNUSED(is_null), char *error) {
    char name[KAFKA_MAX_SETUP_VAR_LEN + 1];
    char value[KAFKA_MAX_SETUP_VAR_LEN + 1];
    char errbuf[KAFKA_ERRBUF_LEN];
    size_t namelen = args->lengths[0];
    size_t valuelen = args->lengths[1];

    debug_print("%s\n", __FUNCTION__);

    /* A NULL here is a genuine SQL NULL, unlike the same test in _init() */
    if (args->args[0] == NULL) {
        error_print("%s: NULL property name\n", __FUNCTION__);
        *error = 1;
        return -1;
    }

    if (namelen == 0 || namelen > KAFKA_MAX_SETUP_VAR_LEN) {
        error_print("%s: Invalid property name length\n", __FUNCTION__);
        *error = 1;
        return -1;
    }

    memcpy(name, args->args[0], namelen);
    name[namelen] = '\0';

    /*
     * A SQL NULL value means "forget this property", which is how a parameter
     * set for an earlier connection is taken back out -- the list now survives
     * kafka_disconnect() so that reconnecting is a single call.
     */
    if (args->args[1] == NULL) {
        if (kafka_table_unset_param(name) != 0) {
            error_print("%s: Cannot unset %s: not set, or already connected\n", __FUNCTION__, name);
            *error = 1;
            return -1;
        }

        return 0;
    }

    if (valuelen > KAFKA_MAX_SETUP_VAR_LEN) {
        error_print("%s: Invalid property value length\n", __FUNCTION__);
        *error = 1;
        return -1;
    }

    /* Counted, not NUL terminated: see the comment in kafka-cmd-send.c */
    memcpy(value, args->args[1], valuelen);
    value[valuelen] = '\0';

    errbuf[0] = '\0';

    if (kafka_table_set_param(name, value, errbuf, sizeof(errbuf)) != 0) {
        /*
         * The value is deliberately absent from this message: it may be a
         * password, and this goes to the server error log.
         */
        error_print("%s: Cannot set %s: %s\n", __FUNCTION__, name, errbuf);
        *error = 1;
        return -1;
    }

    return 0;
}
