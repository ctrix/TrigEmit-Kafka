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
SET(MARIADB_FIND_REQUIRED ON)
SET(MARIADB_FIND_QUIETLY OFF)
INCLUDE(FindMariaDB)
find_package(MariaDB REQUIRED)

INCLUDE(FindKafka)

# *******************************************************************************

CONFIGURE_FILE(
    ${CMAKE_CURRENT_SOURCE_DIR}/config.h.in
    ${CMAKE_CURRENT_BINARY_DIR}/config.h
)

INCLUDE_DIRECTORIES(
    ${MARIADB_INCLUDE_DIR}
    ${RDKAFKA_INCLUDE_DIRS}
)

SET( SOURCES
    kafka-cmd-info.c
    kafka-cmd-stats.c
    kafka-cmd-set_brokers.c
    kafka-cmd-disconnect.c
    kafka-cmd-send.c
    kafka-engine.c
    kafka-table.c
    ht.c
)

SET(LINKLIBS
    ${MARIADB_LIBRARY}
    ${RDKAFKA_LIBRARIES}
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

#SET(DESTDIR "${CMAKE_INSTALL_PREFIX}/${MARIADB_PLUGIN_DIR}")
SET(DESTDIR "${MARIADB_PLUGIN_DIR}")

INSTALL(
    TARGETS
        kafka
    LIBRARY DESTINATION "${DESTDIR}"
    ARCHIVE DESTINATION "${DESTDIR}"
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
