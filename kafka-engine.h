
#ifndef TRIGEMIT_KAFKA_ENGINE_H
#define TRIGEMIT_KAFKA_ENGINE_H 1

#include <kafka.h>

#include <pthread.h>
#include <librdkafka/rdkafka.h>

typedef enum tek_kafka_status_e tek_kafka_status_t;
enum tek_kafka_status_e {
    KAFKA_ERROR = 0,
    KAFKA_SUCCESS = 1
};

typedef struct tek_kafka_s tek_kafka_t;

/* One Kafka producer, bound to a single topic, with its own poll thread. */
struct tek_kafka_s {
    const char *brokers;
    const char *topic;

    rd_kafka_conf_t *conf;      /* Temporary configuration object */
    rd_kafka_topic_conf_t *topic_conf;

    rd_kafka_t *rk;             /* Producer instance handle */
    rd_kafka_topic_t *rkt;      /* Topic object */

    /*
     * Written by the delivery report callback on the poll thread and read by
     * whichever session calls kafka_stats(), so both need to be atomic. They
     * used to be volatile, which orders nothing and does not make the
     * read-modify-write in the callback indivisible.
     */
    _Atomic uint32_t transferred;
    _Atomic uint32_t failed;

    pthread_t thread;

    /*
     * Only touched by the thread that owns the endpoint: set after a
     * successful pthread_create(), cleared in dispose(). Both happen either
     * under the table write lock or before the endpoint is published to the
     * table, so unlike running it needs no atomicity of its own.
     */
    int thread_started;

    /* Cleared by dispose() and polled by the endpoint thread: genuinely shared */
    _Atomic int running;
};

tek_kafka_t *tek_kafka_endpoint_create(const char *brokers, const char *topic);
void tek_kafka_endpoint_dispose(tek_kafka_t *ptk);
tek_kafka_status_t tek_kafka_endpoint_run(tek_kafka_t *ptk);
tek_kafka_status_t tek_kafka_producer_feed(tek_kafka_t *ptk, const void *data, size_t datalen);
void tek_kafka_endpoint_get_stats(tek_kafka_t *ptk, uint32_t *transferred, uint32_t *failed);

#endif
