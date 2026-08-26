
#ifndef TRIGEMIT_KAFKA_TABLE_H
#define TRIGEMIT_KAFKA_TABLE_H 1

#include "kafka-engine.h"

typedef struct kafka_topic_s kafka_topic_t;
struct kafka_topic_s {
    char *topic;
    tek_kafka_t *conn;
};

/*
 * The registry is a flat array of topics searched linearly.
 *
 * That is a deliberate choice, not a shortcut: kafka-engine.c runs one poll
 * thread per topic, which caps the realistic topic count in the low tens --
 * far below where a strcmp scan costs anything next to the produce call it
 * precedes. An array also makes removing a single topic trivial, which is
 * what a hash table without a delete operation could not do at all.
 */
typedef struct kafka_tables_s kafka_tables_t;
struct kafka_tables_s {
    char *brokers;              /* NULL until the brokers are set: doubles as the "initialized" flag */
    kafka_topic_t **topics;
    size_t count;
    size_t capacity;
};

int kafka_table_initialized(void);
int kafka_table_set_brokers(char *name);
/* Copies the configured broker list into buf (always NUL terminated, possibly
   truncated). Returns 1 when the brokers are set, 0 when they are not, in
   which case buf is left empty. */
int kafka_table_get_brokers(char *buf, size_t buflen);

int kafka_table_exists(char *name);

/* Creates the topic's producer if it does not exist yet. Returns 0 when the
   topic is ready (including when it already existed), -1 on failure. No
   kafka_topic_t is handed out: the table owns them and kafka_disconnect()
   can free them at any time. */
int kafka_table_create(char *name);

int kafka_table_send(char *topic, const void *data, size_t datalen);

/* Tears down one topic, leaving the brokers and every other topic in place.
   Returns 0 when the topic was dismissed, -1 when it was not registered. */
int kafka_table_dismiss(char *topic);

void kafka_table_dismiss_all(void);

void kafka_topic_get_stats(char *topic, uint32_t * transferred, uint32_t * failed);

#endif
