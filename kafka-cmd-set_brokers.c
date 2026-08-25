
#include "kafka.h"
#include "kafka-table.h"

#ifdef __cplusplus
extern "C" {
#endif

    DLLEXP my_bool kafka_set_brokers_init(UDF_INIT * initid, UDF_ARGS * args, char *message);
    DLLEXP void kafka_set_brokers_deinit(UDF_INIT * initid);
    DLLEXP long long kafka_set_brokers(UDF_INIT * initid, UDF_ARGS * args, char *is_null, char *error);

#ifdef __cplusplus
};
#endif

my_bool kafka_set_brokers_init(UDF_INIT *UNUSED(initid), UDF_ARGS *args, char *message) {
    debug_print("%s\n", __FUNCTION__);

    if (args->arg_count != 1) {
        strncpy(message, "Wrong arguments to function.", MYSQL_ERRMSG_SIZE);
        return 1;
    }

    if (args->arg_type[0] != STRING_RESULT) {
        strncpy(message, "Wrong arguments. Need: 'broker:port - comma sep.' (string)", MYSQL_ERRMSG_SIZE);
        return 1;
    }

    if (args->args[0] == NULL) {
        strncpy(message, "Wrong arguments. NULL values not allowed.", MYSQL_ERRMSG_SIZE);
        return 1;
    }

    if (args->lengths[0] == 0 || args->lengths[0] > KAFKA_MAX_SETUP_VAR_LEN) {
        strncpy(message, "Wrong arguments. String is too long.", MYSQL_ERRMSG_SIZE);
        return 1;
    }

    if (kafka_table_initialized()) {
        strncpy(message, "Kafka brokers has already been initialized.", MYSQL_ERRMSG_SIZE);
        return 1;
    }

    return 0;
}

void kafka_set_brokers_deinit(UDF_INIT *UNUSED(initid)) {
    debug_print("%s\n", __FUNCTION__);
}

long long kafka_set_brokers(UDF_INIT *UNUSED(initid), UDF_ARGS *args, char *UNUSED(is_null), char *error) {
    /* Counted, not NUL terminated: see the comment in kafka-cmd-send.c */
    char brokers[KAFKA_MAX_SETUP_VAR_LEN + 1];
    size_t brokerslen = args->lengths[0];

    debug_print("%s\n", __FUNCTION__);

    if (args->args[0] == NULL || brokerslen == 0 || brokerslen > KAFKA_MAX_SETUP_VAR_LEN) {
        error_print("%s: Invalid brokers argument\n", __FUNCTION__);
        *error = 1;
        return -1;
    }

    memcpy(brokers, args->args[0], brokerslen);
    brokers[brokerslen] = '\0';

    if (kafka_table_set_brokers(brokers) == 0) {
        return 0;
    } else {
        error_print("%s: Cannot initialize kafka brokers\n", __FUNCTION__);
        *error = 1;
        return -1;
    }

    return 0;
}
