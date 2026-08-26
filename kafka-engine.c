
#include <assert.h>
#include <stdatomic.h>
#include <kafka.h>
#include <kafka-engine.h>

/**
 * @brief Message delivery report callback.
 *
 * This callback is called exactly once per message, indicating if
 * the message was succesfully delivered
 * (rkmessage->err == RD_KAFKA_RESP_ERR_NO_ERROR) or permanently
 * failed delivery (rkmessage->err != RD_KAFKA_RESP_ERR_NO_ERROR).
 *
 * The callback is triggered from rd_kafka_poll() and executes on
 * the application's thread.
 */
static void dr_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) {
    tek_kafka_t *ptk = rkmessage->_private;

    assert(ptk);
    (void) rk;
    (void) opaque;

    if (rkmessage->err) {
        fprintf(stderr, "Message delivery failed: %s\n", rd_kafka_err2str(rkmessage->err));
        atomic_fetch_add(&ptk->failed, 1);
    } else {
        /* TODO OLD Set produce.offset.report to true for the offset to work */
        //fprintf(stderr, "%% Message delivered (%zd bytes, " "partition %" PRId32 " offset %ld)\n", rkmessage->len, rkmessage->partition, rkmessage->offset);
        atomic_fetch_add(&ptk->transferred, 1);
    }

    return;
}

/*
static int stats_cb(rd_kafka_t * rk, char *json, size_t json_len, void *opaque) {
    (void) rk;
    (void) json_len;
    (void) opaque;
    info_print("%s\n", json);
    return 0;
}
*/

/*
 * Sets one librdkafka property, reporting failure instead of asserting it.
 * The previous code used assert(), which NDEBUG removes from Release and
 * RelWithDebInfo builds, so every one of these checks vanished from the
 * builds people actually ship.
 */
static tek_kafka_status_t kafka_conf_set(rd_kafka_conf_t *conf, const char *name, const char *value) {
    char errstr[512];

    if (rd_kafka_conf_set(conf, name, value, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        error_print("Cannot set kafka property %s=%s: %s\n", name, value, errstr);
        return KAFKA_ERROR;
    }

    return KAFKA_SUCCESS;
}

tek_kafka_t *tek_kafka_endpoint_create(const char *brokers, const char *topic) {
    tek_kafka_t *ptk = NULL;

    if (zstr(brokers) || zstr(topic)) {
        return NULL;
    }

    ptk = malloc(sizeof(tek_kafka_t));
    if (ptk == NULL) {
        return NULL;
    }
    memset(ptk, 0, sizeof(tek_kafka_t));

    ptk->running = 0;
    ptk->brokers = strdup(brokers);
    ptk->topic = strdup(topic);

    ptk->conf = rd_kafka_conf_new();
    ptk->topic_conf = rd_kafka_topic_conf_new();

    /*
     * internal.termination.signal is deliberately NOT set.
     *
     * librdkafka's own examples set it to SIGIO to make rd_kafka_destroy()
     * return quickly, but doing so installs a process-wide signal handler,
     * and librdkafka's documentation requires the application to mask that
     * signal. This code runs inside mariadbd, which knows nothing about it:
     * enabling it was measurably flipping SIGIO in the server's SigCgt mask.
     * SIGIO's default action is to terminate the process, and the handler
     * lives in a library the server may unload, so the only thing it buys us
     * -- a faster destroy -- is not worth changing the host's signal
     * disposition. Teardown already waits far longer on the flush anyway.
     */

    if (kafka_conf_set(ptk->conf, "bootstrap.servers", ptk->brokers) != KAFKA_SUCCESS || kafka_conf_set(ptk->conf, "compression.codec", "lz4") != KAFKA_SUCCESS) {
        /* Nothing has been started yet, so dispose() just releases what we hold */
        tek_kafka_endpoint_dispose(ptk);
        return NULL;
    }
    //rd_kafka_conf_set(ptk->conf, "debug", "all", errstr, sizeof(errstr));
    //rd_kafka_conf_set(ptk->conf, "debug", "msg,protocol", errstr, sizeof(errstr));

    /*
       rd_kafka_conf_set(ptk->conf, "statistics.interval.ms", "1000", errstr, sizeof(errstr));
       rd_kafka_conf_set_stats_cb(ptk->conf, stats_cb);
     */

    return ptk;
}

void tek_kafka_endpoint_dispose(tek_kafka_t *ptk) {

    if (ptk == NULL) {
        return;
    }

    ptk->running = 0;

    /*
     * Only join a thread that was really started: pthread_join() on an
     * unset pthread_t is undefined behaviour, and an endpoint whose
     * _run() failed never got that far.
     */
    if (ptk->thread_started) {
        pthread_join(ptk->thread, NULL);
        ptk->thread_started = 0;
    }

    /* rk is NULL when the endpoint was created but never successfully run */
    if (ptk->rk != NULL) {
        int remaining;

        /*
         * rd_kafka_flush() serves delivery reports until the queue drains or
         * the deadline expires, so it is all the draining we need. The loop
         * that used to follow it here spun on rd_kafka_outq_len() with no
         * deadline at all: a queued message only leaves the queue once it gets
         * a delivery report, and against an unreachable broker that does not
         * happen until message.timeout.ms elapses -- 300s by default. That
         * turned kafka_disconnect(), DROP FUNCTION and server shutdown into
         * multi-minute hangs.
         */
        rd_kafka_flush(ptk->rk, KAFKA_FLUSH_TIMEOUT_MS);

        remaining = rd_kafka_outq_len(ptk->rk);
        if (remaining > 0) {
            error_print("Giving up on %d undelivered message(s) for topic %s after %d ms\n", remaining, ptk->topic, KAFKA_FLUSH_TIMEOUT_MS);
        }
    }

    /* Destroy topic object */
    if (ptk->rkt != NULL) {
        rd_kafka_topic_destroy(ptk->rkt);
    }

    /* Destroy the producer instance */
    if (ptk->rk != NULL) {
        rd_kafka_destroy(ptk->rk);
    }

    /*
     * These are non-NULL only while this endpoint still owns them, i.e. when
     * it was created but never successfully run. Once rd_kafka_new() has
     * consumed the conf, or the conf has consumed the topic conf, the
     * corresponding field is NULL and nothing is destroyed twice.
     */
    if (ptk->conf != NULL) {
        rd_kafka_conf_destroy(ptk->conf);
        ptk->conf = NULL;
    }

    if (ptk->topic_conf != NULL) {
        rd_kafka_topic_conf_destroy(ptk->topic_conf);
        ptk->topic_conf = NULL;
    }

    safe_free(ptk->brokers);
    safe_free(ptk->topic);
    safe_free(ptk);

    return;
}

void tek_kafka_endpoint_get_stats(tek_kafka_t *ptk, uint32_t *transferred, uint32_t *failed) {
    if (ptk == NULL) {
        return;
    }

    /*
     * Flushing here is deliberate: it is what makes the counters account for
     * everything produced so far, and the timeout bounds the wait. rk is NULL
     * for an endpoint that was created but never successfully run, so it has
     * nothing queued and nothing to flush.
     */
    if (ptk->rk != NULL) {
        rd_kafka_flush(ptk->rk, KAFKA_FLUSH_TIMEOUT_MS);
    }

    if (transferred != NULL) {
        *transferred = atomic_load(&ptk->transferred);
    }

    if (failed != NULL) {
        *failed = atomic_load(&ptk->failed);
    }

    return;
}

static void *endpoint_thread(void *data) {
    tek_kafka_t *ptk = data;

    while (ptk->running) {
        rd_kafka_poll(ptk->rk, 100 /* 0.1 sec */ );
    }
    return NULL;
}

static tek_kafka_status_t tek_kafka_endpoint_run_producer(tek_kafka_t *ptk) {
    int failed;
    char errstr[1024];

    /* Producer config */
    if (kafka_conf_set(ptk->conf, "max.in.flight.requests.per.connection", "20000") != KAFKA_SUCCESS ||
        kafka_conf_set(ptk->conf, "queue.buffering.max.messages", "50000") != KAFKA_SUCCESS ||
        kafka_conf_set(ptk->conf, "message.send.max.retries", "3") != KAFKA_SUCCESS ||
        kafka_conf_set(ptk->conf, "batch.num.messages", "20000") != KAFKA_SUCCESS || kafka_conf_set(ptk->conf, "queue.buffering.max.ms", "500") != KAFKA_SUCCESS) {
        return KAFKA_ERROR;
    }

/*
    rd = rd_kafka_conf_set(ptk->conf, "queue.buffering.max.kbytes", "2097151", errstr, sizeof(errstr));
    assert(rd == RD_KAFKA_CONF_OK);
    rd = rd_kafka_conf_set(ptk->conf, "queued.max.messages.kbytes", "2097151", errstr, sizeof(errstr));
    assert(rd == RD_KAFKA_CONF_OK);
    rd = rd_kafka_conf_set(ptk->conf, "retry.backoff.ms", "250", NULL, 0);
    assert(rd == RD_KAFKA_CONF_OK);
    //rd = rd_kafka_conf_set(ptk->conf, "request.required.acks", "0", NULL, 0);

    rd = rd_kafka_conf_set(ptk->conf, "message.max.bytes", "2000000", NULL, 0);
    assert(rd == RD_KAFKA_CONF_OK);
    rd_kafka_conf_set(ptk->conf, "debug", "msg,protocol", errstr, sizeof(errstr));
*/

    /* Delivery callback */
    rd_kafka_conf_set_dr_msg_cb(ptk->conf, dr_msg_cb);

    /* Attach the topic conf: conf takes ownership, so stop tracking it here */
    rd_kafka_conf_set_default_topic_conf(ptk->conf, ptk->topic_conf);
    ptk->topic_conf = NULL;

#if 0
    {
        size_t t;
        size_t s = 256;
        const char **c;
        c = rd_kafka_conf_dump(ptk->conf, &s);

        for (t = 0; t < s; t += 2) {
            info_print("%3zu) %-40s %s\n", t / 2, c[t], c[t + 1]);
        }
    }
#endif

    ptk->rk = rd_kafka_new(RD_KAFKA_PRODUCER, ptk->conf, errstr, sizeof(errstr));
    if (ptk->rk == NULL) {
        info_print("%% Failed to create producer: %s\n", errstr);
        /* Ownership of conf passes to rk only on success: destroy it here */
        rd_kafka_conf_destroy(ptk->conf);
        ptk->conf = NULL;
        return KAFKA_ERROR;
    }

    /* rk owns conf now (and, through it, the topic conf) */
    ptk->conf = NULL;

    ptk->rkt = rd_kafka_topic_new(ptk->rk, ptk->topic, NULL);
    if (ptk->rkt == NULL) {
        info_print("%% Failed to create topic object: %s\n", rd_kafka_err2str(rd_kafka_last_error()));
        rd_kafka_destroy(ptk->rk);
        ptk->rk = NULL;
        return KAFKA_ERROR;
    }

    ptk->running = 1;
    failed = pthread_create(&ptk->thread, NULL, endpoint_thread, (void *) ptk);
    if (failed) {
        ptk->running = 0;
        error_print("Cannot start kafka endpoint thread\n");
        return KAFKA_ERROR;
    }
    ptk->thread_started = 1;

    return KAFKA_SUCCESS;
}

tek_kafka_status_t tek_kafka_endpoint_run(tek_kafka_t *ptk) {
    if (ptk == NULL) {
        return KAFKA_ERROR;
    }

    return tek_kafka_endpoint_run_producer(ptk);
}

static tek_kafka_status_t tek_kafka_producer_feed_key(tek_kafka_t *ptk, const void *key, size_t klen, const void *data, size_t datalen) {

    if (key != NULL && klen == 0) {
        klen = strlen((char *) key) + 1;
    }

    do {
        if (rd_kafka_produce(
                                /* Topic object */
                                ptk->rkt,
                                /* Use builtin partitioner to select partition */
                                RD_KAFKA_PARTITION_UA,
                                /* Make a copy of the payload. */
                                RD_KAFKA_MSG_F_COPY,
                                /* Message payload (value) and length */
                                (void *) data, datalen,
                                /* Optional key and its length */
                                //NULL, 0,
                                key, klen,
                                /* Message opaque, provided in
                                 * delivery report callback as
                                 * msg_opaque. */
                                ptk) == -1) {

            /* Poll to handle delivery reports */
            if (rd_kafka_last_error() == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                /* If the internal queue is full, wait for
                 * messages to be delivered and then retry.
                 * The internal queue represents both
                 * messages to be sent and messages that have
                 * been sent or failed, awaiting their
                 * delivery report callback to be called.
                 *
                 * The internal queue is limited by the
                 * configuration property
                 * queue.buffering.max.messages */

                rd_kafka_poll(ptk->rk, 100);
                continue;
            } else {
                info_print("Failed to produce to topic %s: %s\n", rd_kafka_topic_name(ptk->rkt), rd_kafka_err2str(rd_kafka_last_error()));
                return KAFKA_ERROR;
            }
        }
        rd_kafka_poll(ptk->rk, 0);

        break;
    } while (1);

    return KAFKA_SUCCESS;
}

tek_kafka_status_t tek_kafka_producer_feed(tek_kafka_t *ptk, const void *data, size_t datalen) {
    return tek_kafka_producer_feed_key(ptk, NULL, 0, data, datalen);
}
