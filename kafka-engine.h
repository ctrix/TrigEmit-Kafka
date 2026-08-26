
#ifndef TRIGEMIT_KAFKA_ENGINE_H
#define TRIGEMIT_KAFKA_ENGINE_H 1

#include <kafka.h>

#include <pthread.h>
#include <time.h>
#include <librdkafka/rdkafka.h>

typedef enum tek_kafka_status_e tek_kafka_status_t;
enum tek_kafka_status_e {
    KAFKA_ERROR = 0,
    KAFKA_SUCCESS = 1
};

/*
 * One producer for the whole process, with a cheap handle per topic.
 *
 * This is librdkafka's own model: an rd_kafka_t owns the broker connections,
 * the send queue and one poll thread, while an rd_kafka_topic_t is little more
 * than a named handle onto it. Giving every topic its own producer and its own
 * poll thread -- which is what this used to do -- multiplied connections and
 * threads by the topic count and made teardown pay the flush timeout once per
 * topic, serially.
 */
typedef struct tek_kafka_s tek_kafka_t;
struct tek_kafka_s {
    char *brokers;

    /*
     * Owned only until rd_kafka_new() succeeds, which takes it over; NULLed at
     * that moment so dispose() destroys exactly what is still ours. There is
     * deliberately no separate rd_kafka_topic_conf_t: topic-scoped properties
     * are set on this conf and librdkafka forwards them to its own default
     * topic conf. Attaching a topic conf afterwards, as this used to do,
     * replaced that default and silently discarded every topic-scoped property
     * already set.
     */
    rd_kafka_conf_t *conf;
    rd_kafka_t *rk;

    pthread_t thread;

    /*
     * Only touched by the thread that owns the producer: set after a
     * successful pthread_create(), cleared in dispose(). Both happen under the
     * table write lock, so unlike running it needs no atomicity of its own.
     */
    int thread_started;

    /* Cleared by dispose() and polled by the poll thread: genuinely shared */
    _Atomic int running;
};

typedef struct tek_topic_s tek_topic_t;
struct tek_topic_s {
    char *name;
    rd_kafka_topic_t *rkt;
    tek_kafka_t *producer;      /* borrowed, never owned */

    /*
     * Written by the delivery report callback on the poll thread and read by
     * whichever session calls kafka_stats(), so both need to be atomic.
     */
    _Atomic uint32_t transferred;
    _Atomic uint32_t failed;

    /*
     * Delivery failure log throttling. Plain fields on purpose: they are read
     * and written only inside the delivery report callback, which runs on the
     * poll thread while the producer is live and on the disposing thread only
     * after dispose() has joined that thread. The two never overlap, and
     * pthread_join() provides the happens-before, so no atomics and no lock
     * are needed here. Do not "fix" this by adding one.
     */
    rd_kafka_resp_err_t last_err;
    time_t last_log_time;
    uint32_t suppressed;
};

/* Builds the configuration. Nothing connects until tek_kafka_producer_run(). */
tek_kafka_t *tek_kafka_producer_create(const char *brokers);

/*
 * Sets one librdkafka property. Only valid before run(): librdkafka reads the
 * conf at rd_kafka_new() and it is gone afterwards. Rejects unknown names and
 * bad values with librdkafka's own message.
 */
tek_kafka_status_t tek_kafka_producer_set_param(tek_kafka_t * tk, const char *name, const char *value, char *errbuf, size_t errbuflen);

tek_kafka_status_t tek_kafka_producer_run(tek_kafka_t * tk, char *errbuf, size_t errbuflen);

/* Bounded. Returns how many messages were still queued when it gave up. */
int tek_kafka_producer_flush(tek_kafka_t * tk);

/* Every topic handle must be disposed before this is called */
void tek_kafka_producer_dispose(tek_kafka_t * tk);

tek_topic_t *tek_kafka_topic_create(tek_kafka_t * tk, const char *name);

/*
 * Releases the librdkafka handle and reports any throttled delivery failures.
 * The record itself is NOT freed: a message still in flight carries a pointer
 * to it as its delivery report opaque. Free it with tek_kafka_topic_free()
 * only once the producer's out queue is empty, or after the producer has been
 * destroyed -- rd_kafka_destroy() serves every outstanding report first.
 */
void tek_kafka_topic_dispose(tek_topic_t * tt);
void tek_kafka_topic_free(tek_topic_t * tt);

tek_kafka_status_t tek_kafka_topic_feed(tek_topic_t * tt, const void *data, size_t datalen);
void tek_kafka_topic_get_stats(tek_topic_t * tt, uint32_t * transferred, uint32_t * failed);

#endif
