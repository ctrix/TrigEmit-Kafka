
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

    if (args->arg_count != 0) {
        strncpy(message, "Wrong arguments to function.", MYSQL_ERRMSG_SIZE);
        return 1;
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

long long kafka_disconnect(UDF_INIT *UNUSED(initid), UDF_ARGS *UNUSED(args), char *UNUSED(is_null), char *UNUSED(error)) {
    debug_print("%s\n", __FUNCTION__);

    kafka_table_dismiss_all();

    return 0;
}
