CMAKE_MINIMUM_REQUIRED(VERSION 3.16)

SET(CMAKE_MODULE_PATH ${CMAKE_CURRENT_LIST_DIR}/cmake)

# Runs before PROJECT(), which takes the version it derives from the git tag
INCLUDE(VersionString)

PROJECT (TrigEmit-Kafka VERSION ${GIT_VERSION} LANGUAGES C)

SET(CMAKE_SKIP_BUILD_RPATH  FALSE)
SET(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)

#SET(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib/")
#SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH FALSE)

IF(${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_BINARY_DIR})
    MESSAGE(FATAL_ERROR "Prevented in-tree built. This is not a good practice. Please read README.md to find a suitable example.")
ENDIF()

IF (WIN32)
    MESSAGE(FATAL_ERROR "This software does not work on Windows, sorry!")
ENDIF (WIN32)

INCLUDE(DefineCMakeDefaults)
INCLUDE(DefineCompilerFlags)

# **********************************************************
# Compile debug_print() diagnostics in or out. Defaults to on for a Debug
# build and off otherwise; the value is cached from the first configure, so
# changing CMAKE_BUILD_TYPE later does not move it -- pass -DTRIGEMIT_DEBUG
# explicitly, or reconfigure from a clean build directory.

IF(CMAKE_BUILD_TYPE STREQUAL "Debug")
    SET(TRIGEMIT_DEBUG_DEFAULT ON)
ELSE()
    SET(TRIGEMIT_DEBUG_DEFAULT OFF)
ENDIF()

OPTION(TRIGEMIT_DEBUG "Compile in debug_print() diagnostics" ${TRIGEMIT_DEBUG_DEFAULT})

MESSAGE(STATUS "Version: ${PROJECT_VERSION} (${GIT_COMMIT_ID})")
MESSAGE(STATUS "TRIGEMIT_DEBUG: ${TRIGEMIT_DEBUG}")

# **********************************************************

SET(MARIADB_DEBUG OFF)
FIND_PACKAGE(MariaDB REQUIRED)

# librdkafka has no Find module of its own here: PKG_CHECK_MODULES with
# REQUIRED already aborts with a usable message when it is missing, so a
# wrapper around it could only repeat the check. IMPORTED_TARGET gives a
# target carrying the include dirs and libraries together.
FIND_PACKAGE(PkgConfig REQUIRED)
PKG_CHECK_MODULES(RDKAFKA REQUIRED IMPORTED_TARGET rdkafka)

# *******************************************************************************

CONFIGURE_FILE(
    ${CMAKE_CURRENT_SOURCE_DIR}/config.h.in
    ${CMAKE_CURRENT_BINARY_DIR}/config.h
)

INCLUDE_DIRECTORIES(${MARIADB_INCLUDE_DIR})

SET( SOURCES
    kafka-cmd-info.c
    kafka-cmd-stats.c
    kafka-cmd-set_brokers.c
    kafka-cmd-disconnect.c
    kafka-cmd-send.c
    kafka-engine.c
    kafka-table.c
)

# The MariaDB client library is deliberately absent. A UDF is dlopened by the
# server, which resolves the UDF ABI from its own symbols; the build needs the
# MariaDB headers and nothing else from MariaDB. Linking the client library in
# only risks two copies of those symbols in one process.
#
# PkgConfig::RDKAFKA carries librdkafka's include dirs and libraries with it.
SET(LINKLIBS
    PkgConfig::RDKAFKA
)


ADD_LIBRARY (
    kafka
        MODULE ${SOURCES}
)

TARGET_LINK_LIBRARIES (
    kafka
        ${LINKLIBS}
)

SET_TARGET_PROPERTIES(
    kafka
    PROPERTIES
        PREFIX ""
        PROJECT_LABEL "TrigEmit Kafka UDF for MariaDB"
)

# Straight into the server's own plugin directory, which is absolute -- see
# cmake/FindMariaDB.cmake. Staging still works the usual way, with the DESTDIR
# environment variable at install time.
#
# A MODULE library produces no archive, so LIBRARY is the only destination
# that applies.
INSTALL(
    TARGETS
        kafka
    LIBRARY DESTINATION "${MARIADB_PLUGIN_DIR}"
)

# *******************************************************************************
# *** TESTS
# *******************************************************************************

#SET(_USE_TESTS   true CACHE BOOL  "Compile the tests")
#IF ( ${_USE_TESTS} )
#    ENABLE_TESTING()
#    MESSAGE(STATUS "ENABLING TESTS")
#    ADD_SUBDIRECTORY(tests)
#ELSE( ${NMAIL_USE_TESTS} )
#    MESSAGE(STATUS "TESTS NOT ENABLED")
#ENDIF( ${_USE_TESTS} )
