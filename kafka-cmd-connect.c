
#include "kafka.h"
#include "kafka-table.h"

#ifdef __cplusplus
extern "C" {
#endif

    DLLEXP my_bool kafka_connect_init(UDF_INIT * initid, UDF_ARGS * args, char *message);
    DLLEXP void kafka_connect_deinit(UDF_INIT * initid);
    DLLEXP long long kafka_connect(UDF_INIT * initid, UDF_ARGS * args, char *is_null, char *error);

#ifdef __cplusplus
};
#endif

my_bool kafka_connect_init(UDF_INIT *UNUSED(initid), UDF_ARGS *args, char *message) {
    debug_print("%s\n", __FUNCTION__);

    if (args->arg_count != 1) {
        strncpy(message, "Wrong arguments. Need: 'broker:port,...' (string)", MYSQL_ERRMSG_SIZE);
        return 1;
    }

    args->arg_type[0] = STRING_RESULT;

    if (kafka_table_connected()) {
        strncpy(message, "Already connected. Call kafka_disconnect() first.", MYSQL_ERRMSG_SIZE);
        return 1;
    }

    return 0;
}

void kafka_connect_deinit(UDF_INIT *UNUSED(initid)) {
    debug_print("%s\n", __FUNCTION__);
}

long long kafka_connect(UDF_INIT *UNUSED(initid), UDF_ARGS *args, char *UNUSED(is_null), char *error) {
    /* Counted, not NUL terminated: see the comment in kafka-cmd-send.c */
    char brokers[KAFKA_MAX_SETUP_VAR_LEN + 1];
    char errbuf[KAFKA_ERRBUF_LEN];
    size_t brokerslen = args->lengths[0];

    debug_print("%s\n", __FUNCTION__);

    /* A NULL here is a genuine SQL NULL, unlike the same test in _init() */
    if (args->args[0] == NULL || brokerslen == 0 || brokerslen > KAFKA_MAX_SETUP_VAR_LEN) {
        error_print("%s: Invalid brokers argument\n", __FUNCTION__);
        *error = 1;
        return -1;
    }

    memcpy(brokers, args->args[0], brokerslen);
    brokers[brokerslen] = '\0';

    errbuf[0] = '\0';

    if (kafka_table_connect(brokers, errbuf, sizeof(errbuf)) != 0) {
        error_print("%s: Cannot connect: %s\n", __FUNCTION__, errbuf);
        *error = 1;
        return -1;
    }

    info_print("Connected to %s\n", brokers);

    return 0;
}
