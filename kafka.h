
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

#define zstr(x)  ( ((x==NULL) || (*x == 0)) ? 1 : 0)
#define safe_free(x) if(x != NULL) { free( (void*) x); x = NULL; }

#define debug_print(...) \
    if (DEBUG > 0) { \
        fprintf(stderr, PACKAGE_STRING ": " __VA_ARGS__); \
    }

#define info_print(...) \
   do { \
       fprintf(stderr,  PACKAGE_STRING ": " __VA_ARGS__); \
   } while (0)

#define error_print(...) \
   do { \
       fprintf(stderr, PACKAGE_STRING ": " __VA_ARGS__); \
   } while (0)

#endif
