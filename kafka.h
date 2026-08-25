
#ifndef TRIGEMIT_KAFKA_H
#define TRIGEMIT_KAFKA_H 1

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#include <mysql.h>

#if defined(_WIN32) || defined(_WIN64) || defined(__WIN32__) || defined(WIN32)
#define DLLEXP __declspec(dllexport)
#else
#define DLLEXP
#endif

#ifdef UNUSED
#elif defined(__GNUC__)
#define UNUSED(x) UNUSED_ ## x __attribute__((unused))
#elif defined(__LCLINT__)
#define UNUSED(x) /*@unused@*/ x
#else
#define UNUSED(x) x
#endif

/* A function, not a macro: the argument would otherwise be evaluated twice */
static inline int zstr(const char *s) {
    return (s == NULL || *s == '\0') ? 1 : 0;
}

/* Has to stay a macro: it clears the caller's pointer */
#define safe_free(x) \
   do { \
       if ((x) != NULL) { \
           free((void *) (x)); \
           (x) = NULL; \
       } \
   } while (0)

/* The body stays compiled - and its arguments type checked - even when DEBUG
   is 0. The branch is what the optimiser drops. */
#define debug_print(...) \
   do { \
       if (DEBUG > 0) { \
           fprintf(stderr, PACKAGE_NAME ": " __VA_ARGS__); \
       } \
   } while (0)

#define info_print(...) \
   do { \
       fprintf(stderr, PACKAGE_NAME ": " __VA_ARGS__); \
   } while (0)

#define error_print(...) \
   do { \
       fprintf(stderr, PACKAGE_NAME ": " __VA_ARGS__); \
   } while (0)

#endif
