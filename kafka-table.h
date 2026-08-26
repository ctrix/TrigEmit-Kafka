
#ifndef TRIGEMIT_KAFKA_TABLE_H
#define TRIGEMIT_KAFKA_TABLE_H 1

#include "kafka-engine.h"

/*
 * One librdkafka property, held until kafka_connect() builds the producer.
 *
 * Properties cannot be stored straight into a conf object because the conf
 * belongs to the producer and the producer does not exist yet. They are
 * validated as they arrive, against a throwaway conf, so a typo is refused at
 * the moment it is written rather than at connect time.
 */
typedef struct kafka_param_s kafka_param_t;
struct kafka_param_s {
    char *name;
    char *value;
};

/*
 * The registry is a flat array of topics searched linearly.
 *
 * That is a deliberate choice, not a shortcut: topic handles are cheap now
 * that they share one producer, and a strcmp scan costs nothing next to the
 * produce call it precedes. An array also makes removing a single topic
 * trivial, which is what a hash table without a delete operation could not do
 * at all.
 */
typedef struct kafka_tables_s kafka_tables_t;
struct kafka_tables_s {
    tek_kafka_t *producer;      /* NULL until kafka_connect(): the "connected" flag */

    kafka_param_t *params;
    size_t param_count;
    size_t param_capacity;

    tek_topic_t **topics;
    size_t count;
    size_t capacity;

    /*
     * Topics dismissed while messages were still in flight. Their librdkafka
     * handle is gone but the record cannot be freed yet: a queued message
     * carries a pointer to it as its delivery report opaque. Freed once the
     * out queue is empty or the producer has been destroyed.
     */
    tek_topic_t **retired;
    size_t retired_count;
    size_t retired_capacity;
};

/* 1 once kafka_connect() has built a producer, 0 otherwise */
int kafka_table_connected(void);

/*
 * Builds the producer from the accumulated parameters and starts its poll
 * thread. Returns 0 on success, -1 on failure, with errbuf describing why.
 */
int kafka_table_connect(char *brokers, char *errbuf, size_t errbuflen);

/*
 * Checks a property name and value without storing anything. Lets a UDF
 * _init() reject a bad property with a real SQL error message.
 */
int kafka_table_validate_param(char *name, char *value, char *errbuf, size_t errbuflen);

/*
 * Records one librdkafka property to apply at connect time, replacing any
 * previous value for the same name. Refused once connected: librdkafka reads
 * the configuration at connect and never looks at it again.
 */
int kafka_table_set_param(char *name, char *value, char *errbuf, size_t errbuflen);

/* Copies the configured broker list into buf (always NUL terminated, possibly
   truncated). Returns 1 when connected, 0 when not, in which case buf is left
   empty. */
int kafka_table_get_brokers(char *buf, size_t buflen);

/* Renders the parameter list into buf for kafka_info(), with secrets redacted.
   Returns the number of parameters. */
int kafka_table_describe_params(char *buf, size_t buflen);

int kafka_table_exists(char *name);

/* Creates the topic handle if it does not exist yet. Returns 0 when the topic
   is ready (including when it already existed), -1 on failure. No tek_topic_t
   is handed out: the table owns them and kafka_disconnect() can free them at
   any time. */
int kafka_table_create(char *name);

int kafka_table_send(char *topic, const void *data, size_t datalen);

/* Tears down one topic, leaving the producer and every other topic in place.
   Returns 0 when the topic was dismissed, -1 when it was not registered. */
int kafka_table_dismiss(char *topic);

/*
 * Tears down every topic and the producer. The parameters are kept, so a
 * reconnect does not have to re-enter the whole configuration.
 */
void kafka_table_dismiss_all(void);

/* Disconnect plus the parameter list. For module unload. */
void kafka_table_shutdown(void);

/* Forgets one parameter. Refused while connected, like setting one. */
int kafka_table_unset_param(char *name);

void kafka_topic_get_stats(char *topic, uint32_t * transferred, uint32_t * failed);

#endif
