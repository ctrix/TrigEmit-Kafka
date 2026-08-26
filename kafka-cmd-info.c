
#include "kafka.h"
#include "kafka-table.h"

#ifdef __cplusplus
extern "C" {
#endif

    DLLEXP my_bool kafka_info_init(UDF_INIT * initid, UDF_ARGS * args, char *message);
    DLLEXP void kafka_info_deinit(UDF_INIT * initid);
    DLLEXP char *kafka_info(UDF_INIT * initid, UDF_ARGS * args, char *result, unsigned long *length, char *is_null, char *error);

#ifdef __cplusplus
};
#endif

my_bool kafka_info_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
    debug_print("%s\n", __FUNCTION__);

    if (args->arg_count != 0) {
        strncpy(message, "Wrong arguments to function.", MYSQL_ERRMSG_SIZE);
        return 1;
    }

    /*
     * The result buffer the server supplies is only KAFKA_UDF_RESULT_LEN
     * bytes, which a long broker list can overrun, so this function returns a
     * buffer of its own. It is allocated here and released in _deinit().
     */
    initid->ptr = malloc(KAFKA_INFO_MAX_LEN);
    if (initid->ptr == NULL) {
        strncpy(message, "Out of memory.", MYSQL_ERRMSG_SIZE);
        return 1;
    }

    initid->max_length = KAFKA_INFO_MAX_LEN;
    initid->maybe_null = 0;
    initid->const_item = 0;

    return 0;
}

void kafka_info_deinit(UDF_INIT *initid) {
    debug_print("%s\n", __FUNCTION__);

    safe_free(initid->ptr);
}

char *kafka_info(UDF_INIT *initid, UDF_ARGS *UNUSED(args), char *UNUSED(result), unsigned long *length, char *UNUSED(is_null), char *UNUSED(error)) {
    char *buf = initid->ptr;
    char brokers[KAFKA_MAX_SETUP_VAR_LEN + 1];
    char params[KAFKA_INFO_MAX_LEN / 2];
    const char *brk = brokers;
    int nparams;
    int written;

    debug_print("%s\n", __FUNCTION__);

    /* Copied out under the table lock: a concurrent kafka_disconnect() frees
       the stored string, so it must not be held by pointer */
    if (kafka_table_get_brokers(brokers, sizeof(brokers)) == 0) {
        brk = "Not connected";
    }

    nparams = kafka_table_describe_params(params, sizeof(params));

    written = snprintf(buf, KAFKA_INFO_MAX_LEN, "Version: %s\nBrokers: %s\nParameters: %d\n%s", PACKAGE_STRING, brk, nparams, params);

    /* snprintf() reports what it would have written, so clamp to what it did */
    if (written < 0) {
        written = 0;
    } else if (written >= KAFKA_INFO_MAX_LEN) {
        written = KAFKA_INFO_MAX_LEN - 1;
    }

    *length = (unsigned long) written;

    return buf;
}
