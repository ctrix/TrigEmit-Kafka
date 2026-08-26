
#include "kafka.h"
#include "kafka-table.h"

/*
 * A read/write lock rather than a mutex: sending is a read of the table
 * (look the topic up, hand its endpoint to librdkafka, which is itself
 * thread safe), so any number of sessions may produce at the same time.
 * Only creating a topic and tearing one down need exclusive access.
 *
 * The important rule here is that a kafka_topic_t pointer must never leave
 * the lock. kafka_disconnect() disposes and frees every topic, so a caller
 * holding a bare pointer across an unlock would be reading freed memory.
 * Nothing in this file returns one to the outside.
 */
static pthread_rwlock_t ktlock = PTHREAD_RWLOCK_INITIALIZER;

/* Slots allocated the first time a topic is registered, doubling thereafter */
#define KAFKA_TABLE_INITIAL_SLOTS 8

static kafka_tables_t kt = {
    NULL,                       // brokers
    NULL,                       // topics
    0,                          // count
    0                           // capacity
};

/* Caller must hold ktlock, for reading or for writing */
static kafka_topic_t *kafka_table_find_locked(const char *topic) {
    size_t i;

    if (topic == NULL) {
        return NULL;
    }

    for (i = 0; i < kt.count; i++) {
        if (strcmp(kt.topics[i]->topic, topic) == 0) {
            return kt.topics[i];
        }
    }

    return NULL;
}

/* Caller must hold ktlock for writing. Returns 0 on success, -1 on failure,
   in which case the table is unchanged and the caller still owns t. */
static int kafka_table_insert_locked(kafka_topic_t *t) {
    if (kt.count == kt.capacity) {
        size_t newcap = (kt.capacity == 0) ? KAFKA_TABLE_INITIAL_SLOTS : kt.capacity * 2;
        kafka_topic_t **grown = realloc(kt.topics, newcap * sizeof(*grown));

        if (grown == NULL) {
            return -1;
        }

        kt.topics = grown;
        kt.capacity = newcap;
    }

    kt.topics[kt.count++] = t;

    return 0;
}

/*
 * Caller must hold ktlock for writing. Disposes the topic and takes it out of
 * the array by moving the last entry into the hole, which is why nothing may
 * depend on the order of kt.topics.
 */
static void kafka_table_remove_locked(size_t idx) {
    kafka_topic_t *t = kt.topics[idx];

    kt.topics[idx] = kt.topics[kt.count - 1];
    kt.count--;

    tek_kafka_endpoint_dispose(t->conn);
    safe_free(t->topic);
    safe_free(t);
}

int kafka_table_initialized(void) {
    pthread_rwlock_rdlock(&ktlock);
    int res = (kt.brokers != NULL);
    pthread_rwlock_unlock(&ktlock);

    return res;
}

static int kafka_table_initialize(char *brokers) {
    pthread_rwlock_wrlock(&ktlock);

    if (kt.brokers != NULL) {
        debug_print("%s: Brokers already set\n", __FUNCTION__);
        pthread_rwlock_unlock(&ktlock);
        return -1;
    }

    kt.brokers = strdup(brokers);
    if (kt.brokers == NULL) {
        pthread_rwlock_unlock(&ktlock);
        error_print("%s: Error allocating memory for the broker list\n", __FUNCTION__);
        return -2;
    }

    pthread_rwlock_unlock(&ktlock);

    return 0;
}

int kafka_table_set_brokers(char *brokers) {
    int res = kafka_table_initialize(brokers);

    if (res == -1) {
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

    if (kt.brokers == NULL) {
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

    if (kafka_table_insert_locked(t) != 0) {
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
     * use, because tearing a topic down needs the write lock.
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

int kafka_table_dismiss(char *topic) {
    size_t i;
    int res = -1;

    if (zstr(topic)) {
        error_print("%s: Empty topic name\n", __FUNCTION__);
        return -1;
    }

    pthread_rwlock_wrlock(&ktlock);

    for (i = 0; i < kt.count; i++) {
        if (strcmp(kt.topics[i]->topic, topic) == 0) {
            info_print("Disconnecting topic %s\n", topic);
            kafka_table_remove_locked(i);
            res = 0;
            break;
        }
    }

    pthread_rwlock_unlock(&ktlock);

    if (res != 0) {
        error_print("Cannot disconnect topic %s: not connected\n", topic);
    }

    return res;
}

void kafka_table_dismiss_all(void) {
    pthread_rwlock_wrlock(&ktlock);

    /*
     * Removing from the end keeps the swap in kafka_table_remove_locked() a
     * no-op, so each topic is disposed exactly once.
     */
    while (kt.count > 0) {
        info_print("Disconnecting topic %s\n", kt.topics[kt.count - 1]->topic);
        kafka_table_remove_locked(kt.count - 1);
    }

    safe_free(kt.topics);
    kt.capacity = 0;

    safe_free(kt.brokers);

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
 * Without this nothing tears the producers down on the way out, so whatever
 * librdkafka still had queued is simply lost when mariadbd exits, unless an
 * operator remembered to call kafka_disconnect() first.
 */
static void __attribute__((destructor)) kafka_module_unload(void) {
    info_print("Module unloading, disconnecting from kafka\n");

    kafka_table_dismiss_all();
}
