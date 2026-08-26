
#include "kafka.h"
#include "kafka-table.h"

/*
 * A read/write lock rather than a mutex: sending is a read of the table (look
 * the topic up, hand its handle to librdkafka, which is itself thread safe),
 * so any number of sessions may produce at the same time. Only connecting,
 * creating a topic and tearing down need exclusive access.
 *
 * The important rule here is that a tek_topic_t pointer must never leave the
 * lock. kafka_disconnect() disposes and frees every topic, so a caller holding
 * a bare pointer across an unlock would be reading freed memory. Nothing in
 * this file returns one to the outside.
 */
static pthread_rwlock_t ktlock = PTHREAD_RWLOCK_INITIALIZER;

/* Slots allocated on first use, doubling thereafter */
#define KAFKA_TABLE_INITIAL_SLOTS 8

static kafka_tables_t kt = {
    NULL,                       // producer
    NULL, 0, 0,                 // params
    NULL, 0, 0,                 // topics
    NULL, 0, 0                  // retired
};

/* ---------------------------------------------------------------- helpers */

/* Grows any of the pointer arrays above by doubling. Caller holds the write
   lock. Returns 0 on success, -1 when the allocation failed and the array is
   left untouched. */
static int kafka_table_grow(void **array, size_t *capacity, size_t count, size_t itemsize) {
    void *grown;
    size_t newcap;

    if (count < *capacity) {
        return 0;
    }

    newcap = (*capacity == 0) ? KAFKA_TABLE_INITIAL_SLOTS : *capacity * 2;

    grown = realloc(*array, newcap * itemsize);
    if (grown == NULL) {
        return -1;
    }

    *array = grown;
    *capacity = newcap;

    return 0;
}

/* Caller must hold ktlock, for reading or for writing */
static tek_topic_t *kafka_table_find_locked(const char *topic) {
    size_t i;

    if (topic == NULL) {
        return NULL;
    }

    for (i = 0; i < kt.count; i++) {
        if (strcmp(kt.topics[i]->name, topic) == 0) {
            return kt.topics[i];
        }
    }

    return NULL;
}

/*
 * Frees the retired topic records once nothing can still be pointing at them.
 * A record is referenced by any message of its topic that is still queued, as
 * the delivery report opaque, and there is no per-topic queue length -- so an
 * empty out queue for the whole producer is the condition. Caller holds the
 * write lock.
 */
static void kafka_table_reap_retired_locked(void) {
    size_t i;

    if (kt.retired_count == 0) {
        return;
    }

    if (kt.producer != NULL && kt.producer->rk != NULL && rd_kafka_outq_len(kt.producer->rk) > 0) {
        return;
    }

    for (i = 0; i < kt.retired_count; i++) {
        tek_kafka_topic_free(kt.retired[i]);
    }

    kt.retired_count = 0;

    return;
}

/*
 * Takes a topic out of the array and releases its librdkafka handle. The
 * record itself is freed only if the out queue has drained; otherwise it is
 * parked on the retired list, because a message still in flight names it as
 * its delivery report opaque. Caller holds the write lock.
 */
static void kafka_table_remove_locked(size_t idx) {
    tek_topic_t *tt = kt.topics[idx];

    /* The last entry fills the hole, so nothing may depend on the order */
    kt.topics[idx] = kt.topics[kt.count - 1];
    kt.count--;

    tek_kafka_topic_dispose(tt);

    if (kt.producer != NULL && kt.producer->rk != NULL && rd_kafka_outq_len(kt.producer->rk) > 0) {
        if (kafka_table_grow((void **) &kt.retired, &kt.retired_capacity, kt.retired_count, sizeof(*kt.retired)) == 0) {
            kt.retired[kt.retired_count++] = tt;
            return;
        }

        /*
         * Nowhere to park it. Leaking one small record beats freeing memory
         * the delivery report callback is about to read.
         */
        error_print("Cannot retire topic record, leaking it rather than risking a use-after-free\n");
        return;
    }

    tek_kafka_topic_free(tt);

    return;
}

/* Caller holds the write lock */
static void kafka_table_clear_params_locked(void) {
    size_t i;

    for (i = 0; i < kt.param_count; i++) {
        safe_free(kt.params[i].name);
        safe_free(kt.params[i].value);
    }

    safe_free(kt.params);
    kt.param_count = 0;
    kt.param_capacity = 0;

    return;
}

/* ------------------------------------------------------------ connection */

int kafka_table_connected(void) {
    int res;

    pthread_rwlock_rdlock(&ktlock);
    res = (kt.producer != NULL);
    pthread_rwlock_unlock(&ktlock);

    return res;
}

/*
 * Checks a property against a throwaway conf without storing it. Exposed so a
 * UDF _init() can reject a bad property with a real SQL error message, which
 * the main function cannot do -- its char *error is a single byte.
 */
int kafka_table_validate_param(char *name, char *value, char *errbuf, size_t errbuflen) {
    rd_kafka_conf_t *scratch;
    char errstr[512];

    if (zstr(name) || value == NULL) {
        snprintf(errbuf, errbuflen, "Parameter name must not be empty");
        return -1;
    }

    scratch = rd_kafka_conf_new();
    if (scratch == NULL) {
        snprintf(errbuf, errbuflen, "Out of memory");
        return -1;
    }

    if (rd_kafka_conf_set(scratch, name, value, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        snprintf(errbuf, errbuflen, "%s", errstr);
        rd_kafka_conf_destroy(scratch);
        return -1;
    }

    rd_kafka_conf_destroy(scratch);

    return 0;
}

int kafka_table_set_param(char *name, char *value, char *errbuf, size_t errbuflen) {
    size_t i;
    char *dupname;
    char *dupvalue;
    int res = -1;

    /*
     * Validated before it is stored, so an unknown property or a bad value is
     * reported here rather than at connect time when the context is gone.
     */
    if (kafka_table_validate_param(name, value, errbuf, errbuflen) != 0) {
        return -1;
    }

    pthread_rwlock_wrlock(&ktlock);

    if (kt.producer != NULL) {
        snprintf(errbuf, errbuflen, "Already connected: disconnect before changing parameters");
        goto done;
    }

    /* Setting the same property again replaces it rather than stacking up */
    for (i = 0; i < kt.param_count; i++) {
        if (strcmp(kt.params[i].name, name) == 0) {
            dupvalue = strdup(value);
            if (dupvalue == NULL) {
                snprintf(errbuf, errbuflen, "Out of memory");
                goto done;
            }

            safe_free(kt.params[i].value);
            kt.params[i].value = dupvalue;
            res = 0;
            goto done;
        }
    }

    if (kafka_table_grow((void **) &kt.params, &kt.param_capacity, kt.param_count, sizeof(*kt.params)) != 0) {
        snprintf(errbuf, errbuflen, "Out of memory");
        goto done;
    }

    dupname = strdup(name);
    dupvalue = strdup(value);
    if (dupname == NULL || dupvalue == NULL) {
        safe_free(dupname);
        safe_free(dupvalue);
        snprintf(errbuf, errbuflen, "Out of memory");
        goto done;
    }

    kt.params[kt.param_count].name = dupname;
    kt.params[kt.param_count].value = dupvalue;
    kt.param_count++;
    res = 0;

 done:
    pthread_rwlock_unlock(&ktlock);

    return res;
}

int kafka_table_connect(char *brokers, char *errbuf, size_t errbuflen) {
    tek_kafka_t *tk = NULL;
    size_t i;
    int res = -1;

    if (zstr(brokers)) {
        snprintf(errbuf, errbuflen, "Broker list must not be empty");
        return -1;
    }

    pthread_rwlock_wrlock(&ktlock);

    if (kt.producer != NULL) {
        snprintf(errbuf, errbuflen, "Already connected: call kafka_disconnect() first");
        goto done;
    }

    tk = tek_kafka_producer_create(brokers);
    if (tk == NULL) {
        snprintf(errbuf, errbuflen, "Cannot create the producer configuration");
        goto done;
    }

    /*
     * Applied after the defaults, so naming a property that has a default
     * overrides it rather than colliding with it.
     */
    for (i = 0; i < kt.param_count; i++) {
        if (tek_kafka_producer_set_param(tk, kt.params[i].name, kt.params[i].value, errbuf, errbuflen) != KAFKA_SUCCESS) {
            tek_kafka_producer_dispose(tk);
            goto done;
        }
    }

    if (tek_kafka_producer_run(tk, errbuf, errbuflen) != KAFKA_SUCCESS) {
        tek_kafka_producer_dispose(tk);
        goto done;
    }

    kt.producer = tk;
    res = 0;

 done:
    pthread_rwlock_unlock(&ktlock);

    return res;
}

int kafka_table_get_brokers(char *buf, size_t buflen) {
    int res = 0;

    if (buf == NULL || buflen == 0) {
        return 0;
    }

    /*
     * The broker list is copied out under the lock rather than returned by
     * pointer: kafka_disconnect() can free it at any moment, and a caller
     * holding the bare pointer would be reading freed memory.
     */
    pthread_rwlock_rdlock(&ktlock);

    if (kt.producer != NULL && kt.producer->brokers != NULL) {
        snprintf(buf, buflen, "%s", kt.producer->brokers);
        res = 1;
    } else {
        buf[0] = '\0';
    }

    pthread_rwlock_unlock(&ktlock);

    return res;
}

/* A property whose value must never reach a log or a result set */
static int kafka_param_is_secret(const char *name) {
    return (strstr(name, "password") != NULL || strstr(name, "secret") != NULL);
}

int kafka_table_describe_params(char *buf, size_t buflen) {
    size_t i;
    size_t used = 0;
    int count;

    if (buf == NULL || buflen == 0) {
        return 0;
    }

    buf[0] = '\0';

    pthread_rwlock_rdlock(&ktlock);

    for (i = 0; i < kt.param_count && used + 1 < buflen; i++) {
        int written = snprintf(buf + used, buflen - used, "  %s = %s\n", kt.params[i].name, kafka_param_is_secret(kt.params[i].name) ? "********" : kt.params[i].value);

        if (written < 0) {
            break;
        }

        used += (size_t) written;
    }

    count = (int) kt.param_count;

    pthread_rwlock_unlock(&ktlock);

    return count;
}

/* ---------------------------------------------------------------- topics */

int kafka_table_exists(char *topic) {
    int res;

    pthread_rwlock_rdlock(&ktlock);
    res = (kafka_table_find_locked(topic) != NULL);
    pthread_rwlock_unlock(&ktlock);

    return res;
}

int kafka_table_create(char *topic_name) {
    tek_topic_t *tt = NULL;
    int res = -1;

    pthread_rwlock_wrlock(&ktlock);

    /*
     * The lookup and the insert happen under one write lock, so two sessions
     * racing to send to the same new topic cannot both build a handle and
     * leave one of them orphaned in the table.
     */
    if (kafka_table_find_locked(topic_name) != NULL) {
        res = 0;
        goto done;
    }

    if (kt.producer == NULL) {
        error_print("%s: Not connected\n", __FUNCTION__);
        goto done;
    }

    if (kafka_table_grow((void **) &kt.topics, &kt.capacity, kt.count, sizeof(*kt.topics)) != 0) {
        error_print("Error allocating memory for the topic table\n");
        goto done;
    }

    tt = tek_kafka_topic_create(kt.producer, topic_name);
    if (tt == NULL) {
        error_print("Error creating the topic handle\n");
        goto done;
    }

    kt.topics[kt.count++] = tt;
    res = 0;

    debug_print("%s: TOPIC %s is starting up\n", __FUNCTION__, topic_name);

 done:
    /* A convenient moment to release anything dismissed earlier */
    kafka_table_reap_retired_locked();

    pthread_rwlock_unlock(&ktlock);

    return res;
}

int kafka_table_send(char *topic, const void *data, size_t datalen) {
    tek_topic_t *tt = NULL;
    tek_kafka_status_t status;

    debug_print("%s\n", __FUNCTION__);

    /*
     * Held for reading across the produce call. Concurrent senders do not
     * block each other, and the handle cannot be disposed while it is in use,
     * because tearing a topic down needs the write lock.
     */
    pthread_rwlock_rdlock(&ktlock);

    tt = kafka_table_find_locked(topic);
    if (tt == NULL) {
        pthread_rwlock_unlock(&ktlock);
        error_print("Error sending to a non existing topic\n");
        return -1;
    }

    status = tek_kafka_topic_feed(tt, data, datalen);

    pthread_rwlock_unlock(&ktlock);

    if (status != KAFKA_SUCCESS) {
        error_print("Error sending data to kafka\n");
        return -1;
    }

    return (int) datalen;
}

int kafka_table_dismiss(char *topic) {
    size_t i;
    int res = -1;

    if (zstr(topic)) {
        error_print("%s: Empty topic name\n", __FUNCTION__);
        return -1;
    }

    pthread_rwlock_wrlock(&ktlock);

    for (i = 0; i < kt.count; i++) {
        if (strcmp(kt.topics[i]->name, topic) == 0) {
            info_print("Disconnecting topic %s\n", topic);

            /*
             * Drain first, bounded: it gives this topic's queued messages a
             * chance to be delivered and, when it succeeds, lets the record be
             * freed immediately instead of being parked.
             */
            tek_kafka_producer_flush(kt.producer);

            kafka_table_remove_locked(i);
            res = 0;
            break;
        }
    }

    kafka_table_reap_retired_locked();

    pthread_rwlock_unlock(&ktlock);

    if (res != 0) {
        error_print("Cannot disconnect topic %s: not connected\n", topic);
    }

    return res;
}

void kafka_table_dismiss_all(void) {
    size_t i;
    int remaining;

    pthread_rwlock_wrlock(&ktlock);

    if (kt.producer != NULL) {
        /* One flush covers every topic: there is a single queue */
        remaining = tek_kafka_producer_flush(kt.producer);
        if (remaining > 0) {
            error_print("Giving up on %d undelivered message(s) after %d ms\n", remaining, KAFKA_FLUSH_TIMEOUT_MS);
        }
    }

    /* Release the librdkafka handles while the producer is still alive */
    for (i = 0; i < kt.count; i++) {
        info_print("Disconnecting topic %s\n", kt.topics[i]->name);
        tek_kafka_topic_dispose(kt.topics[i]);
    }

    /*
     * rd_kafka_destroy() inside dispose() serves every outstanding delivery
     * report before returning, so after this point nothing can still be
     * holding a pointer to a topic record and they can all be freed.
     */
    tek_kafka_producer_dispose(kt.producer);
    kt.producer = NULL;

    for (i = 0; i < kt.count; i++) {
        tek_kafka_topic_free(kt.topics[i]);
    }
    safe_free(kt.topics);
    kt.count = 0;
    kt.capacity = 0;

    for (i = 0; i < kt.retired_count; i++) {
        tek_kafka_topic_free(kt.retired[i]);
    }
    safe_free(kt.retired);
    kt.retired_count = 0;
    kt.retired_capacity = 0;

    /*
     * The parameters deliberately survive a disconnect, so reconnecting is
     * just kafka_connect() again rather than re-entering the whole
     * configuration. kafka_table_shutdown() is what finally releases them.
     */

    pthread_rwlock_unlock(&ktlock);

    return;
}

/*
 * Disconnect plus the parameter list. Only for module unload: an operator
 * clears individual parameters with kafka_table_unset_param().
 */
void kafka_table_shutdown(void) {
    kafka_table_dismiss_all();

    pthread_rwlock_wrlock(&ktlock);
    kafka_table_clear_params_locked();
    pthread_rwlock_unlock(&ktlock);

    return;
}

int kafka_table_unset_param(char *name) {
    size_t i;
    int res = -1;

    if (zstr(name)) {
        return -1;
    }

    pthread_rwlock_wrlock(&ktlock);

    if (kt.producer != NULL) {
        goto done;
    }

    for (i = 0; i < kt.param_count; i++) {
        if (strcmp(kt.params[i].name, name) == 0) {
            safe_free(kt.params[i].name);
            safe_free(kt.params[i].value);

            /* The last entry fills the hole: order is not significant */
            kt.params[i] = kt.params[kt.param_count - 1];
            kt.param_count--;
            res = 0;
            break;
        }
    }

 done:
    pthread_rwlock_unlock(&ktlock);

    return res;
}

void kafka_topic_get_stats(char *topic, uint32_t *transferred, uint32_t *failed) {
    tek_topic_t *tt = NULL;

    if (transferred != NULL) {
        *transferred = 0;
    }

    if (failed != NULL) {
        *failed = 0;
    }

    pthread_rwlock_rdlock(&ktlock);

    tt = kafka_table_find_locked(topic);
    if (tt != NULL) {
        tek_kafka_topic_get_stats(tt, transferred, failed);
    }

    pthread_rwlock_unlock(&ktlock);

    return;
}

/*
 * Runs when the module is unloaded: on DROP FUNCTION, and at server shutdown
 * for a library that is still loaded.
 *
 * Without this nothing tears the producer down on the way out, so whatever
 * librdkafka still had queued is simply lost when mariadbd exits, unless an
 * operator remembered to call kafka_disconnect() first.
 */
static void __attribute__((destructor)) kafka_module_unload(void) {
    info_print("Module unloading, disconnecting from kafka\n");

    kafka_table_shutdown();
}
