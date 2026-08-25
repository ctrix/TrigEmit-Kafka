
#include "kafka-table.h"

/*
 * A read/write lock rather than a mutex: sending is a read of the table
 * (look the topic up, hand its endpoint to librdkafka, which is itself
 * thread safe), so any number of sessions may produce at the same time.
 * Only creating a topic and tearing everything down need exclusive access.
 *
 * The important rule here is that a kafka_topic_t pointer must never leave
 * the lock. kafka_disconnect() disposes and frees every topic, so a caller
 * holding a bare pointer across an unlock would be reading freed memory.
 * Nothing in this file returns one to the outside.
 */
static pthread_rwlock_t ktlock = PTHREAD_RWLOCK_INITIALIZER;

static kafka_tables_t kt = {
    NULL,                       // brokers
    NULL                        // table
};

/* Caller must hold ktlock, for reading or for writing */
static kafka_topic_t *kafka_table_find_locked(const char *topic) {
    if (kt.table == NULL) {
        return NULL;
    }

    return ht_get(kt.table, topic);
}

int kafka_table_initialized(void) {
    pthread_rwlock_rdlock(&ktlock);
    int res = (kt.brokers != NULL);
    pthread_rwlock_unlock(&ktlock);

    return res;
}

static int kafka_table_initialize(char *brokers) {
    pthread_rwlock_wrlock(&ktlock);

    if (kt.table != NULL) {
        debug_print("%s: Hash table already exists\n", __FUNCTION__);
        pthread_rwlock_unlock(&ktlock);
        return -1;
    }

    if (kt.brokers != NULL) {
        debug_print("%s: Brokers already set\n", __FUNCTION__);
        pthread_rwlock_unlock(&ktlock);
        return -2;
    }

    kt.table = ht_create();
    kt.brokers = strdup(brokers);

    pthread_rwlock_unlock(&ktlock);

    return 0;
}

int kafka_table_set_brokers(char *brokers) {
    int res = kafka_table_initialize(brokers);

    if (res != 0) {
        error_print("%s: Cannot set brokers as they're already set\n", __FUNCTION__);
    }

    return res;
}

int kafka_table_get_brokers(char *buf, size_t buflen) {
    int res = 0;

    if (buf == NULL || buflen == 0) {
        return 0;
    }

    /*
     * The broker list is copied out under the lock rather than returned by
     * pointer: kafka_table_dismiss_all() can free it at any moment, and a
     * caller holding the bare pointer would be reading freed memory.
     */
    pthread_rwlock_rdlock(&ktlock);

    if (kt.brokers != NULL) {
        snprintf(buf, buflen, "%s", kt.brokers);
        res = 1;
    } else {
        buf[0] = '\0';
    }

    pthread_rwlock_unlock(&ktlock);

    return res;
}

int kafka_table_exists(char *topic) {
    int res;

    pthread_rwlock_rdlock(&ktlock);
    res = (kafka_table_find_locked(topic) != NULL);
    pthread_rwlock_unlock(&ktlock);

    return res;
}

int kafka_table_create(char *topic_name) {
    kafka_topic_t *t = NULL;
    tek_kafka_status_t status;

    pthread_rwlock_wrlock(&ktlock);

    /*
     * The lookup and the insert happen under one write lock, so two sessions
     * racing to send to the same new topic cannot both build an endpoint and
     * leave one of them orphaned in the table.
     */
    if (kafka_table_find_locked(topic_name) != NULL) {
        pthread_rwlock_unlock(&ktlock);
        return 0;
    }

    if (kt.table == NULL) {
        pthread_rwlock_unlock(&ktlock);
        error_print("%s: Brokers have not been set\n", __FUNCTION__);
        return -1;
    }

    t = malloc(sizeof(kafka_topic_t));
    if (t == NULL) {
        pthread_rwlock_unlock(&ktlock);
        error_print("Error allocating memory for new topic\n");
        return -1;
    }

    /* Zeroed so the cleanup path can tell what has been built so far */
    memset(t, 0, sizeof(kafka_topic_t));

    t->topic = strdup(topic_name);
    if (t->topic == NULL) {
        error_print("Error allocating memory for topic name\n");
        goto fail;
    }

    t->conn = tek_kafka_endpoint_create(kt.brokers, topic_name);
    if (t->conn == NULL) {
        error_print("Error creating connection for topic\n");
        goto fail;
    }

    status = tek_kafka_endpoint_run(t->conn);
    if (status != KAFKA_SUCCESS) {
        error_print("Error starting up new connection for topic\n");
        goto fail;
    }

    if (ht_set(kt.table, t->topic, t) == NULL) {
        error_print("Error registering the topic in the table\n");
        goto fail;
    }

    pthread_rwlock_unlock(&ktlock);

    debug_print("%s: TOPIC %s is starting up\n", __FUNCTION__, topic_name);

    return 0;

  fail:
    /*
     * Single cleanup path. The topic never made it into the table, so it can
     * be torn down after unlocking. dispose() returns immediately on a NULL
     * endpoint and tolerates one that was never run, so this is safe from any
     * of the failures above, including after the producer has started.
     */
    pthread_rwlock_unlock(&ktlock);

    tek_kafka_endpoint_dispose(t->conn);
    safe_free(t->topic);
    safe_free(t);

    return -1;
}

int kafka_table_send(char *topic, const void *data, size_t datalen) {
    kafka_topic_t *t = NULL;
    tek_kafka_status_t status;

    debug_print("%s\n", __FUNCTION__);

    /*
     * Held for reading across the produce call. Concurrent senders do not
     * block each other, and the endpoint cannot be disposed while it is in
     * use, because kafka_table_dismiss_all() needs the write lock.
     */
    pthread_rwlock_rdlock(&ktlock);

    t = kafka_table_find_locked(topic);
    if (t == NULL) {
        pthread_rwlock_unlock(&ktlock);
        error_print("Error sending to a non existing topic\n");
        return -1;
    }

    status = tek_kafka_producer_feed(t->conn, data, datalen);

    pthread_rwlock_unlock(&ktlock);

    if (status != KAFKA_SUCCESS) {
        error_print("Error sending data to kafka\n");
        return -1;
    }

    return (int) datalen;
}

void kafka_table_dismiss_all(void) {
    pthread_rwlock_wrlock(&ktlock);

    if (kt.table != NULL) {
        hti it;

        it = ht_iterator(kt.table);
        while (ht_next(&it)) {
            kafka_topic_t *t;
            info_print("Disconnecting topic %s\n", it.key);
            t = it.value;
            tek_kafka_endpoint_dispose(t->conn);
            safe_free(t->topic);
            safe_free(t);
        }

        ht_destroy(kt.table);
        kt.table = NULL;

        safe_free(kt.brokers);
    }

    pthread_rwlock_unlock(&ktlock);
}

void kafka_topic_get_stats(char *topic, uint32_t *transferred, uint32_t *failed) {
    kafka_topic_t *t = NULL;

    if (transferred != NULL) {
        *transferred = 0;
    }

    if (failed != NULL) {
        *failed = 0;
    }

    pthread_rwlock_rdlock(&ktlock);

    t = kafka_table_find_locked(topic);
    if (t != NULL) {
        tek_kafka_endpoint_get_stats(t->conn, transferred, failed);
    }

    pthread_rwlock_unlock(&ktlock);

    return;
}

/*
 * Runs when the module is unloaded: on DROP FUNCTION, and at server shutdown
 * for a library that is still loaded.
 *
 * Without this nothing ever tears the producers down on the way out, so
 * whatever librdkafka still had queued was simply lost when mariadbd exited.
 * kafka_disconnect() did the job, but only if an operator remembered to call
 * it first -- which is exactly what the original project had to document.
 */
static void __attribute__((destructor)) kafka_module_unload(void) {
    info_print("Module unloading, disconnecting from kafka\n");

    kafka_table_dismiss_all();
}
