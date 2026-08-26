
#include <stdatomic.h>
#include <kafka.h>
#include <kafka-engine.h>

/*
 * Delivery failure logging, throttled.
 *
 * A single unreachable broker retires everything librdkafka has queued as a
 * delivery failure apiece, and a trigger under load keeps refilling that
 * queue. Logging one line per message turns a broker outage into a
 * disk-filling event, on an unbuffered stderr written from the poll thread.
 *
 * The count is not the interesting part anyway: tt->failed already has it and
 * kafka_stats() already reports it to SQL. What the log has to carry is the
 * error string and the moment the state changed, so that is what it carries.
 *
 * A change of error code bypasses the interval: going from "broker down" to
 * "message too large" is a different failure and an operator should see it at
 * once rather than up to KAFKA_ERROR_LOG_INTERVAL_S later.
 *
 * Runs only inside dr_msg_cb(); see the note on the throttling fields in
 * kafka-engine.h for why none of this needs locking.
 */
static void log_delivery_failure(tek_topic_t *tt, rd_kafka_resp_err_t err) {
    time_t now = time(NULL);

    if (err == tt->last_err && (now - tt->last_log_time) < KAFKA_ERROR_LOG_INTERVAL_S) {
        tt->suppressed++;
        return;
    }

    if (tt->suppressed > 0) {
        error_print("Delivery failed for topic %s: %s (and %u more in the last %llds)\n", tt->name, rd_kafka_err2str(err), tt->suppressed, (long long) (now - tt->last_log_time));
    } else {
        error_print("Delivery failed for topic %s: %s\n", tt->name, rd_kafka_err2str(err));
    }

    tt->last_err = err;
    tt->last_log_time = now;
    tt->suppressed = 0;
}

/*
 * Called on the first success after a run of failures. Nothing marked the end
 * of an outage before this, so the log showed it starting and never stopping.
 */
static void log_delivery_recovered(tek_topic_t *tt) {
    if (tt->suppressed > 0) {
        info_print("Delivery recovered for topic %s (%u further failure(s) went unreported)\n", tt->name, tt->suppressed);
    } else {
        info_print("Delivery recovered for topic %s\n", tt->name);
    }

    tt->last_err = RD_KAFKA_RESP_ERR_NO_ERROR;
    tt->last_log_time = 0;
    tt->suppressed = 0;
}

static void dr_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) {
    tek_topic_t *tt = rkmessage->_private;

    (void) rk;
    (void) opaque;

    /*
     * Not an assert(): NDEBUG is defined in RelWithDebInfo and Release, so an
     * assert here would be absent from every shipping build and a NULL would
     * become a segfault inside a librdkafka callback.
     */
    if (tt == NULL) {
        return;
    }

    if (rkmessage->err) {
        atomic_fetch_add(&tt->failed, 1);
        log_delivery_failure(tt, rkmessage->err);
    } else {
        if (tt->last_err != RD_KAFKA_RESP_ERR_NO_ERROR) {
            log_delivery_recovered(tt);
        }
        atomic_fetch_add(&tt->transferred, 1);
    }

    return;
}

/*
 * Sets one librdkafka property, reporting failure instead of asserting it.
 * NDEBUG is defined in RelWithDebInfo and Release, so an assert here would be
 * absent from the builds people actually ship.
 *
 * errbuf, when given, receives librdkafka's own message so a UDF can put it in
 * front of the user rather than only in the error log.
 */
static tek_kafka_status_t kafka_conf_set(rd_kafka_conf_t *conf, const char *name, const char *value, char *errbuf, size_t errbuflen) {
    char errstr[512];

    if (rd_kafka_conf_set(conf, name, value, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        if (errbuf != NULL && errbuflen > 0) {
            snprintf(errbuf, errbuflen, "%s", errstr);
        }
        error_print("Cannot set kafka property %s: %s\n", name, errstr);
        return KAFKA_ERROR;
    }

    return KAFKA_SUCCESS;
}

tek_kafka_t *tek_kafka_producer_create(const char *brokers) {
    tek_kafka_t *tk = NULL;

    if (zstr(brokers)) {
        return NULL;
    }

    tk = malloc(sizeof(tek_kafka_t));
    if (tk == NULL) {
        return NULL;
    }
    memset(tk, 0, sizeof(tek_kafka_t));

    tk->brokers = strdup(brokers);
    tk->conf = rd_kafka_conf_new();

    if (tk->brokers == NULL || tk->conf == NULL) {
        tek_kafka_producer_dispose(tk);
        return NULL;
    }

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

    /*
     * Defaults only. Every one of these is an ordinary property name, so a
     * kafka_connection_param() call naming the same property overrides it --
     * these are set first precisely so that it can.
     */
    if (kafka_conf_set(tk->conf, "bootstrap.servers", tk->brokers, NULL, 0) != KAFKA_SUCCESS ||
        kafka_conf_set(tk->conf, "compression.type", "lz4", NULL, 0) != KAFKA_SUCCESS ||
        kafka_conf_set(tk->conf, "queue.buffering.max.messages", "50000", NULL, 0) != KAFKA_SUCCESS ||
        kafka_conf_set(tk->conf, "enable.idempotence", "true", NULL, 0) != KAFKA_SUCCESS ||
        kafka_conf_set(tk->conf, "batch.num.messages", "20000", NULL, 0) != KAFKA_SUCCESS || kafka_conf_set(tk->conf, "queue.buffering.max.ms", "500", NULL, 0) != KAFKA_SUCCESS) {
        tek_kafka_producer_dispose(tk);
        return NULL;
    }

    return tk;
}

tek_kafka_status_t tek_kafka_producer_set_param(tek_kafka_t *tk, const char *name, const char *value, char *errbuf, size_t errbuflen) {
    if (tk == NULL || zstr(name)) {
        return KAFKA_ERROR;
    }

    /*
     * librdkafka reads the conf when rd_kafka_new() is called and the object
     * is gone afterwards, so there is no such thing as changing a property on
     * a running producer. Refusing here is what makes that visible instead of
     * accepting a value that would never take effect.
     */
    if (tk->conf == NULL) {
        if (errbuf != NULL && errbuflen > 0) {
            snprintf(errbuf, errbuflen, "Already connected: disconnect before changing parameters");
        }
        return KAFKA_ERROR;
    }

    return kafka_conf_set(tk->conf, name, value, errbuf, errbuflen);
}

static void *producer_thread(void *data) {
    tek_kafka_t *tk = data;

    while (tk->running) {
        rd_kafka_poll(tk->rk, 100 /* 0.1 sec */ );
    }

    return NULL;
}

tek_kafka_status_t tek_kafka_producer_run(tek_kafka_t *tk, char *errbuf, size_t errbuflen) {
    char errstr[1024];
    int failed;

    if (tk == NULL || tk->conf == NULL) {
        return KAFKA_ERROR;
    }

    rd_kafka_conf_set_dr_msg_cb(tk->conf, dr_msg_cb);

    tk->rk = rd_kafka_new(RD_KAFKA_PRODUCER, tk->conf, errstr, sizeof(errstr));
    if (tk->rk == NULL) {
        /*
         * librdkafka rejects incompatible combinations here rather than at
         * conf_set time -- "`acks` must be set to `all` when
         * `enable.idempotence` is true", for instance -- so this message is
         * the only place the real reason appears. Hand it back to the caller.
         */
        if (errbuf != NULL && errbuflen > 0) {
            snprintf(errbuf, errbuflen, "%s", errstr);
        }
        error_print("Failed to create producer: %s\n", errstr);
        /* Ownership of conf passes to rk only on success: destroy it here */
        rd_kafka_conf_destroy(tk->conf);
        tk->conf = NULL;
        return KAFKA_ERROR;
    }

    /* rk owns conf now */
    tk->conf = NULL;

    tk->running = 1;
    failed = pthread_create(&tk->thread, NULL, producer_thread, (void *) tk);
    if (failed) {
        tk->running = 0;
        if (errbuf != NULL && errbuflen > 0) {
            snprintf(errbuf, errbuflen, "Cannot start the poll thread");
        }
        error_print("Cannot start kafka poll thread\n");
        return KAFKA_ERROR;
    }
    tk->thread_started = 1;

    return KAFKA_SUCCESS;
}

int tek_kafka_producer_flush(tek_kafka_t *tk) {
    if (tk == NULL || tk->rk == NULL) {
        return 0;
    }

    /*
     * rd_kafka_flush() serves delivery reports until the queue drains or the
     * deadline expires. Bounded on purpose: a queued message only leaves the
     * queue once it gets a delivery report, and against an unreachable broker
     * that does not happen until message.timeout.ms elapses -- 300s by
     * default. An unbounded wait here turns kafka_disconnect(), DROP FUNCTION
     * and server shutdown into multi-minute hangs.
     *
     * One flush covers every topic, because there is one queue.
     */
    rd_kafka_flush(tk->rk, KAFKA_FLUSH_TIMEOUT_MS);

    return rd_kafka_outq_len(tk->rk);
}

void tek_kafka_producer_dispose(tek_kafka_t *tk) {
    if (tk == NULL) {
        return;
    }

    tk->running = 0;

    /*
     * Only join a thread that was really started: pthread_join() on an unset
     * pthread_t is undefined behaviour, and a producer whose run() failed
     * never got that far.
     */
    if (tk->thread_started) {
        pthread_join(tk->thread, NULL);
        tk->thread_started = 0;
    }

    /*
     * rd_kafka_destroy() serves every outstanding delivery report before it
     * returns, which is what makes it safe to free the topic records after
     * this point and not before.
     */
    if (tk->rk != NULL) {
        rd_kafka_destroy(tk->rk);
        tk->rk = NULL;
    }

    /* Non-NULL only while we still own it, i.e. rd_kafka_new() never ran */
    if (tk->conf != NULL) {
        rd_kafka_conf_destroy(tk->conf);
        tk->conf = NULL;
    }

    safe_free(tk->brokers);
    safe_free(tk);

    return;
}

tek_topic_t *tek_kafka_topic_create(tek_kafka_t *tk, const char *name) {
    tek_topic_t *tt = NULL;

    if (tk == NULL || tk->rk == NULL || zstr(name)) {
        return NULL;
    }

    tt = malloc(sizeof(tek_topic_t));
    if (tt == NULL) {
        return NULL;
    }
    memset(tt, 0, sizeof(tek_topic_t));

    tt->producer = tk;
    tt->name = strdup(name);
    if (tt->name == NULL) {
        safe_free(tt);
        return NULL;
    }

    /*
     * NULL topic conf: the producer's conf already carries the topic-scoped
     * properties, and librdkafka uses it as the default for every topic.
     */
    tt->rkt = rd_kafka_topic_new(tk->rk, name, NULL);
    if (tt->rkt == NULL) {
        error_print("Failed to create topic object for %s: %s\n", name, rd_kafka_err2str(rd_kafka_last_error()));
        safe_free(tt->name);
        safe_free(tt);
        return NULL;
    }

    return tt;
}

void tek_kafka_topic_dispose(tek_topic_t *tt) {
    if (tt == NULL) {
        return;
    }

    if (tt->rkt != NULL) {
        rd_kafka_topic_destroy(tt->rkt);
        tt->rkt = NULL;
    }

    /*
     * The throttle in the delivery report callback may still be holding
     * failures that were never printed. Report them rather than lose them.
     */
    if (tt->suppressed > 0) {
        error_print("Topic %s: %u further delivery failure(s) went unreported (%s)\n", tt->name, tt->suppressed, rd_kafka_err2str(tt->last_err));
        tt->suppressed = 0;
    }

    return;
}

void tek_kafka_topic_free(tek_topic_t *tt) {
    if (tt == NULL) {
        return;
    }

    safe_free(tt->name);
    safe_free(tt);

    return;
}

void tek_kafka_topic_get_stats(tek_topic_t *tt, uint32_t *transferred, uint32_t *failed) {
    if (tt == NULL) {
        return;
    }

    /*
     * Flushing here is deliberate: it is what makes the counters account for
     * everything produced so far, and the timeout bounds the wait.
     */
    tek_kafka_producer_flush(tt->producer);

    if (transferred != NULL) {
        *transferred = atomic_load(&tt->transferred);
    }

    if (failed != NULL) {
        *failed = atomic_load(&tt->failed);
    }

    return;
}

static tek_kafka_status_t tek_kafka_topic_feed_key(tek_topic_t *tt, const void *key, size_t klen, const void *data, size_t datalen) {
    rd_kafka_t *rk = tt->producer->rk;

    if (key != NULL && klen == 0) {
        klen = strlen((char *) key) + 1;
    }

    do {
        if (rd_kafka_produce(
                                /* Topic object */
                                tt->rkt,
                                /* Use builtin partitioner to select partition */
                                RD_KAFKA_PARTITION_UA,
                                /* Make a copy of the payload. */
                                RD_KAFKA_MSG_F_COPY,
                                /* Message payload (value) and length */
                                (void *) data, datalen,
                                /* Optional key and its length */
                                key, klen,
                                /* Message opaque, handed back to the delivery
                                 * report callback as rkmessage->_private */
                                tt) == -1) {

            if (rd_kafka_last_error() == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                /*
                 * The internal queue holds both messages waiting to be sent
                 * and messages awaiting their delivery report, and is bounded
                 * by queue.buffering.max.messages. Poll to drain reports and
                 * retry.
                 */
                rd_kafka_poll(rk, 100);
                continue;
            } else {
                error_print("Failed to produce to topic %s: %s\n", tt->name, rd_kafka_err2str(rd_kafka_last_error()));
                return KAFKA_ERROR;
            }
        }

        rd_kafka_poll(rk, 0);

        break;
    } while (1);

    return KAFKA_SUCCESS;
}

tek_kafka_status_t tek_kafka_topic_feed(tek_topic_t *tt, const void *data, size_t datalen) {
    if (tt == NULL || tt->rkt == NULL || tt->producer == NULL || tt->producer->rk == NULL) {
        return KAFKA_ERROR;
    }

    return tek_kafka_topic_feed_key(tt, NULL, 0, data, datalen);
}
