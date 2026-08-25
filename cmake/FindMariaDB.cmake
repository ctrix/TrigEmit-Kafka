#
# This module is designed to find/handle mariadb(client) library
#
# Requirements:
# - CMake >= 2.8.3 (for new version of find_package_handle_standard_args)
#
# The following variables will be defined for your use:
#   - MARIADB_INCLUDE_DIRS  : mariadb(client) include directory
#   - MARIADB_LIBRARIES     : mariadb(client) libraries
#   - MARIADB_VERSION       : complete version of mariadb(client) (x.y.z)
#   - MARIADB_VERSION_MAJOR : major version of mariadb(client)
#   - MARIADB_VERSION_MINOR : minor version of mariadb(client)
#   - MARIADB_VERSION_PATCH : patch version of mariadb(client)
#
# How to use:
#   1) Copy this file in the root of your project source directory
#   2) Then, tell CMake to search this non-standard module in your project directory by adding to your CMakeLists.txt:
#        set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR})
#   3) Finally call find_package(MariaDB) once
#
# Here is a complete sample to build an executable:
#
#   set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR})
#
#   find_package(MariaDB REQUIRED) # Note: name is case sensitive
#
#   add_executable(myapp myapp.c)
#   include_directories(${MARIADB_INCLUDE_DIRS})
#   target_link_libraries(myapp ${MARIADB_LIBRARIES})
#   # with CMake >= 3.0.0, the last two lines can be replaced by the following
#   target_link_libraries(myapp MariaDB::MariaDB) # Note: case also matters here
#


#=============================================================================
# Copyright (c) 2013-2020, julp
#
# Distributed under the OSI-approved BSD License
#
# This software is distributed WITHOUT ANY WARRANTY; without even the
# implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#=============================================================================

# TODO:
# - mariadb support?
# - on Windows, lookup for related registry keys


# "As of MariaDB 5.7.9, MariaDB distributions contain a mariadbclient.pc file that provides information about MariaDB configuration for use by the pkg-config command."
find_package(PkgConfig QUIET)

########## Private ##########
if(NOT DEFINED MARIADB_PUBLIC_VAR_NS)
    set(MARIADB_PUBLIC_VAR_NS "MARIADB")
endif(NOT DEFINED MARIADB_PUBLIC_VAR_NS)

if(NOT DEFINED MARIADB_PRIVATE_VAR_NS)
    set(MARIADB_PRIVATE_VAR_NS "_${MARIADB_PUBLIC_VAR_NS}")
endif(NOT DEFINED MARIADB_PRIVATE_VAR_NS)

if(NOT DEFINED PC_MARIADB_PRIVATE_VAR_NS)
    set(PC_MARIADB_PRIVATE_VAR_NS "_PC${MARIADB_PRIVATE_VAR_NS}")
endif(NOT DEFINED PC_MARIADB_PRIVATE_VAR_NS)

# Alias all MariaDB_FIND_X variables to MARIADB_FIND_X
# Workaround for find_package: no way to force case of variable's names it creates (I don't want to change MY coding standard)
set(${MARIADB_PRIVATE_VAR_NS}_FIND_PKG_PREFIX "MariaDB")
get_directory_property(${MARIADB_PRIVATE_VAR_NS}_CURRENT_VARIABLES VARIABLES)

foreach(${MARIADB_PRIVATE_VAR_NS}_VARNAME ${${MARIADB_PRIVATE_VAR_NS}_CURRENT_VARIABLES})
    if(${MARIADB_PRIVATE_VAR_NS}_VARNAME MATCHES "^${${MARIADB_PRIVATE_VAR_NS}_FIND_PKG_PREFIX}")
        string(REGEX REPLACE "^${${MARIADB_PRIVATE_VAR_NS}_FIND_PKG_PREFIX}" "${MARIADB_PUBLIC_VAR_NS}" ${MARIADB_PRIVATE_VAR_NS}_NORMALIZED_VARNAME ${${MARIADB_PRIVATE_VAR_NS}_VARNAME})
        set(${${MARIADB_PRIVATE_VAR_NS}_NORMALIZED_VARNAME} ${${${MARIADB_PRIVATE_VAR_NS}_VARNAME}})
    endif(${MARIADB_PRIVATE_VAR_NS}_VARNAME MATCHES "^${${MARIADB_PRIVATE_VAR_NS}_FIND_PKG_PREFIX}")
endforeach(${MARIADB_PRIVATE_VAR_NS}_VARNAME)

macro(_mariadb_set_dotted_version VERSION_STRING)
    set(${MARIADB_PUBLIC_VAR_NS}_VERSION "${VERSION_STRING}")
    string(REGEX MATCHALL "[0-9]+" ${MARIADB_PRIVATE_VAR_NS}_VERSION_PARTS ${VERSION_STRING})
    list(GET ${MARIADB_PRIVATE_VAR_NS}_VERSION_PARTS 0 ${MARIADB_PUBLIC_VAR_NS}_VERSION_MAJOR)
    list(GET ${MARIADB_PRIVATE_VAR_NS}_VERSION_PARTS 1 ${MARIADB_PUBLIC_VAR_NS}_VERSION_MINOR)
    list(GET ${MARIADB_PRIVATE_VAR_NS}_VERSION_PARTS 2 ${MARIADB_PUBLIC_VAR_NS}_VERSION_PATCH)
endmacro(_mariadb_set_dotted_version)

########## Public ##########
if(PKG_CONFIG_FOUND)
    pkg_check_modules(${PC_MARIADB_PRIVATE_VAR_NS} "mariadbclient" QUIET)
    if(${PC_MARIADB_PRIVATE_VAR_NS}_FOUND)
        if(${PC_MARIADB_PRIVATE_VAR_NS}_VERSION)
            _mariadb_set_dotted_version("${${PC_MARIADB_PRIVATE_VAR_NS}_VERSION}")
        endif(${PC_MARIADB_PRIVATE_VAR_NS}_VERSION)
    endif(${PC_MARIADB_PRIVATE_VAR_NS}_FOUND)
endif(PKG_CONFIG_FOUND)

find_program(${MARIADB_PUBLIC_VAR_NS}_CONFIG_EXECUTABLE mariadb_config)
if(${MARIADB_PUBLIC_VAR_NS}_CONFIG_EXECUTABLE)
    execute_process(OUTPUT_STRIP_TRAILING_WHITESPACE COMMAND ${${MARIADB_PUBLIC_VAR_NS}_CONFIG_EXECUTABLE} --cflags                 OUTPUT_VARIABLE ${MARIADB_PUBLIC_VAR_NS}_MARIADB_CONFIG_C_FLAGS)
    execute_process(OUTPUT_STRIP_TRAILING_WHITESPACE COMMAND ${${MARIADB_PUBLIC_VAR_NS}_CONFIG_EXECUTABLE} --version                OUTPUT_VARIABLE ${MARIADB_PUBLIC_VAR_NS}_MARIADB_CONFIG_VERSION)
    execute_process(OUTPUT_STRIP_TRAILING_WHITESPACE COMMAND ${${MARIADB_PUBLIC_VAR_NS}_CONFIG_EXECUTABLE} --variable=pkglibdir     OUTPUT_VARIABLE ${MARIADB_PUBLIC_VAR_NS}_MARIADB_CONFIG_LIBRARY_DIR)
    execute_process(OUTPUT_STRIP_TRAILING_WHITESPACE COMMAND ${${MARIADB_PUBLIC_VAR_NS}_CONFIG_EXECUTABLE} --variable=pkgincludedir OUTPUT_VARIABLE ${MARIADB_PUBLIC_VAR_NS}_MARIADB_CONFIG_INCLUDE_DIR)
    execute_process(OUTPUT_STRIP_TRAILING_WHITESPACE COMMAND ${${MARIADB_PUBLIC_VAR_NS}_CONFIG_EXECUTABLE} --plugindir              OUTPUT_VARIABLE ${MARIADB_PUBLIC_VAR_NS}_MARIADB_CONFIG_PLUGIN_DIR)
    set(${MARIADB_PUBLIC_VAR_NS}_PLUGIN_DIR ${${MARIADB_PUBLIC_VAR_NS}_MARIADB_CONFIG_PLUGIN_DIR})
#     execute_process(OUTPUT_STRIP_TRAILING_WHITESPACE COMMAND ${${MARIADB_PUBLIC_VAR_NS}_CONFIG_EXECUTABLE} --socket                 OUTPUT_VARIABLE ${MARIADB_PUBLIC_VAR_NS}_MARIADB_CONFIG_SOCKET)
#     execute_process(OUTPUT_STRIP_TRAILING_WHITESPACE COMMAND ${${MARIADB_PUBLIC_VAR_NS}_CONFIG_EXECUTABLE} --port                   OUTPUT_VARIABLE ${MARIADB_PUBLIC_VAR_NS}_MARIADB_CONFIG_PORT)
#     execute_process(OUTPUT_STRIP_TRAILING_WHITESPACE COMMAND ${${MARIADB_PUBLIC_VAR_NS}_CONFIG_EXECUTABLE} --libmariadbd-libs         OUTPUT_VARIABLE ${MARIADB_PUBLIC_VAR_NS}_MARIADB_CONFIG_LIBRARY_EMBEDDED)
    _mariadb_set_dotted_version("${${MARIADB_PUBLIC_VAR_NS}_MARIADB_CONFIG_VERSION}")
endif(${MARIADB_PUBLIC_VAR_NS}_CONFIG_EXECUTABLE)

set(${MARIADB_PRIVATE_VAR_NS}_COMMON_FIND_OPTIONS PATH_SUFFIXES mariadb)

find_path(
    ${MARIADB_PUBLIC_VAR_NS}_INCLUDE_DIR
    NAMES mariadb_version.h
    ${${MARIADB_PRIVATE_VAR_NS}_COMMON_FIND_OPTIONS}
    PATHS ${${PC_MARIADB_PRIVATE_VAR_NS}_INCLUDE_DIRS} ${${MARIADB_PUBLIC_VAR_NS}_MARIADB_CONFIG_INCLUDE_DIR}
)

IF (NOT ${MARIADB_PUBLIC_VAR_NS}_INCLUDE_DIR)
    # STRETCH WORKAROUND -  look for a different file
    find_path(
        ${MARIADB_PUBLIC_VAR_NS}_INCLUDE_DIR
        NAMES my_global.h
        ${${MARIADB_PRIVATE_VAR_NS}_COMMON_FIND_OPTIONS}
        PATHS ${${PC_MARIADB_PRIVATE_VAR_NS}_INCLUDE_DIRS} ${${MARIADB_PUBLIC_VAR_NS}_MARIADB_CONFIG_INCLUDE_DIR}
    )
ENDIF (NOT ${MARIADB_PUBLIC_VAR_NS}_INCLUDE_DIR) 

if(WIN32)
    include(SelectLibraryConfigurations)
    # "On Windows, the static library is mariadbclient.lib and the dynamic library is libmariadb.dll. Windows distributions also include libmariadb.lib, a static import library needed for using the dynamic library."
    set(${MARIADB_PRIVATE_VAR_NS}_POSSIBLE_NAMES "mariadb" "mariadbclient")

    find_library(
        ${MARIADB_PUBLIC_VAR_NS}_LIBRARY_RELEASE
        NAMES ${${MARIADB_PRIVATE_VAR_NS}_POSSIBLE_NAMES}
        DOC "Release library for mariadbclient"
        ${${MARIADB_PRIVATE_VAR_NS}_COMMON_FIND_OPTIONS}
    )
    # "Windows distributions also include a set of debug libraries. These have the same names as the nondebug libraries, but are located in the lib/debug library. You must use the debug libraries when compiling clients built using the debug C runtime."
    find_library(
        ${MARIADB_PUBLIC_VAR_NS}_LIBRARY_DEBUG
        NAMES ${${MARIADB_PRIVATE_VAR_NS}_POSSIBLE_NAMES}
        DOC "Debug library for mariadbclient"
        PATH_SUFFIXES mariadb/debug debug
    )

    select_library_configurations("${MARIADB_PUBLIC_VAR_NS}")
else(WIN32)
    # "On Unix (and Unix-like) sytems, the static library is libmariadbclient.a. The dynamic library is libmariadbclient.so on most Unix systems and libmariadbclient.dylib on OS X."
    find_library(
        ${MARIADB_PUBLIC_VAR_NS}_LIBRARY
        NAMES mariadbclient
        PATHS ${${PC_MARIADB_PRIVATE_VAR_NS}_LIBRARY_DIRS} ${${MARIADB_PUBLIC_VAR_NS}_MARIADB_CONFIG_LIBRARY_DIR}
        ${${MARIADB_PRIVATE_VAR_NS}_COMMON_FIND_OPTIONS}
    )
endif(WIN32)

if(${MARIADB_PUBLIC_VAR_NS}_INCLUDE_DIR AND NOT ${MARIADB_PUBLIC_VAR_NS}_VERSION)
    file(STRINGS "${${MARIADB_PUBLIC_VAR_NS}_INCLUDE_DIR}/mariadb_version.h" ${MARIADB_PRIVATE_VAR_NS}_VERSION_NUMBER_DEFINITION LIMIT_COUNT 1 REGEX ".*#[ \t]*define[ \t]*MARIADB_VERSION_ID[ \t]*[0-9]+.*")
    string(REGEX REPLACE ".*# *define +MARIADB_VERSION_ID +([0-9]+).*" "\\1" ${MARIADB_PRIVATE_VAR_NS}_VERSION_NUMBER ${${MARIADB_PRIVATE_VAR_NS}_VERSION_NUMBER_DEFINITION})

    math(EXPR ${MARIADB_PUBLIC_VAR_NS}_VERSION_MAJOR "${${MARIADB_PRIVATE_VAR_NS}_VERSION_NUMBER} / 10000")
    math(EXPR ${MARIADB_PUBLIC_VAR_NS}_VERSION_MINOR "(${${MARIADB_PRIVATE_VAR_NS}_VERSION_NUMBER} - ${${MARIADB_PUBLIC_VAR_NS}_VERSION_MAJOR} * 10000) / 100")
    math(EXPR ${MARIADB_PUBLIC_VAR_NS}_VERSION_PATCH "${${MARIADB_PRIVATE_VAR_NS}_VERSION_NUMBER} - ${${MARIADB_PUBLIC_VAR_NS}_VERSION_MAJOR} * 10000 - ${${MARIADB_PUBLIC_VAR_NS}_VERSION_MINOR} * 100")
    set(${MARIADB_PUBLIC_VAR_NS}_VERSION "${${MARIADB_PUBLIC_VAR_NS}_VERSION_MAJOR}.${${MARIADB_PUBLIC_VAR_NS}_VERSION_MINOR}.${${MARIADB_PUBLIC_VAR_NS}_VERSION_PATCH}")
endif(${MARIADB_PUBLIC_VAR_NS}_INCLUDE_DIR AND NOT ${MARIADB_PUBLIC_VAR_NS}_VERSION)

# Check find_package arguments
include(FindPackageHandleStandardArgs)
# The package name must match the file name (FindMariaDB.cmake), or CMake
# warns that the result variables will not follow the expected pattern.
# FPHSA sets both MariaDB_FOUND and the uppercase MARIADB_FOUND, and picks
# up REQUIRED/QUIET from the find_package() call on its own.
find_package_handle_standard_args(
    MariaDB
    REQUIRED_VARS ${MARIADB_PUBLIC_VAR_NS}_LIBRARY ${MARIADB_PUBLIC_VAR_NS}_INCLUDE_DIR
    VERSION_VAR ${MARIADB_PUBLIC_VAR_NS}_VERSION
)

if(${MARIADB_PUBLIC_VAR_NS}_FOUND)
    # <deprecated>
    # for compatibility with previous versions, alias old MARIADB_(MAJOR|MINOR|PATCH)_VERSION to MARIADB_VERSION_$1
    set(${MARIADB_PUBLIC_VAR_NS}_MAJOR_VERSION ${${MARIADB_PUBLIC_VAR_NS}_VERSION_MAJOR})
    set(${MARIADB_PUBLIC_VAR_NS}_MINOR_VERSION ${${MARIADB_PUBLIC_VAR_NS}_VERSION_MINOR})
    set(${MARIADB_PUBLIC_VAR_NS}_PATCH_VERSION ${${MARIADB_PUBLIC_VAR_NS}_VERSION_PATCH})
    # </deprecated>
    set(${MARIADB_PUBLIC_VAR_NS}_LIBRARIES ${${MARIADB_PUBLIC_VAR_NS}_LIBRARY})
    set(${MARIADB_PUBLIC_VAR_NS}_INCLUDE_DIRS ${${MARIADB_PUBLIC_VAR_NS}_INCLUDE_DIR})
    if(CMAKE_VERSION VERSION_GREATER "3.0.0")
        if(NOT TARGET MariaDB::MariaDB)
            add_library(MariaDB::MariaDB UNKNOWN IMPORTED)
        endif(NOT TARGET MariaDB::MariaDB)
        if(${MARIADB_PUBLIC_VAR_NS}_LIBRARY_RELEASE)
            set_property(TARGET MariaDB::MariaDB APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
            set_target_properties(MariaDB::MariaDB PROPERTIES IMPORTED_LOCATION_RELEASE "${${MARIADB_PUBLIC_VAR_NS}_LIBRARY_RELEASE}")
        endif(${MARIADB_PUBLIC_VAR_NS}_LIBRARY_RELEASE)
        if(${MARIADB_PUBLIC_VAR_NS}_LIBRARY_DEBUG)
            set_property(TARGET MariaDB::MariaDB APPEND PROPERTY IMPORTED_CONFIGURATIONS DEBUG)
            set_target_properties(MariaDB::MariaDB PROPERTIES IMPORTED_LOCATION_DEBUG "${${MARIADB_PUBLIC_VAR_NS}_LIBRARY_DEBUG}")
        endif(${MARIADB_PUBLIC_VAR_NS}_LIBRARY_DEBUG)
        if(${MARIADB_PUBLIC_VAR_NS}_LIBRARY)
            set_target_properties(MariaDB::MariaDB PROPERTIES IMPORTED_LOCATION "${${MARIADB_PUBLIC_VAR_NS}_LIBRARY}")
        endif(${MARIADB_PUBLIC_VAR_NS}_LIBRARY)
        set_target_properties(MariaDB::MariaDB PROPERTIES INTERFACE_INCLUDE_DIRECTORIES "${${MARIADB_PUBLIC_VAR_NS}_INCLUDE_DIR}")
    endif(CMAKE_VERSION VERSION_GREATER "3.0.0")
endif(${MARIADB_PUBLIC_VAR_NS}_FOUND)

mark_as_advanced(
    ${MARIADB_PUBLIC_VAR_NS}_INCLUDE_DIR
    ${MARIADB_PUBLIC_VAR_NS}_LIBRARY
)

########## <debug> ##########

if(${MARIADB_PUBLIC_VAR_NS}_DEBUG)

    function(mariadb_debug _VARNAME)
        if(DEFINED ${MARIADB_PUBLIC_VAR_NS}_${_VARNAME})
            message("${MARIADB_PUBLIC_VAR_NS}_${_VARNAME} = ${${MARIADB_PUBLIC_VAR_NS}_${_VARNAME}}")
        else(DEFINED ${MARIADB_PUBLIC_VAR_NS}_${_VARNAME})
            message("${MARIADB_PUBLIC_VAR_NS}_${_VARNAME} = <UNDEFINED>")
        endif(DEFINED ${MARIADB_PUBLIC_VAR_NS}_${_VARNAME})
    endfunction(mariadb_debug)

    # IN (args)
    mariadb_debug("FIND_REQUIRED")
    mariadb_debug("FIND_QUIETLY")
    mariadb_debug("FIND_VERSION")
    # OUT
    # Linking
    mariadb_debug("INCLUDE_DIRS")
    mariadb_debug("LIBRARIES")
    # Version
    mariadb_debug("VERSION_MAJOR")
    mariadb_debug("VERSION_MINOR")
    mariadb_debug("VERSION_PATCH")
    mariadb_debug("VERSION")
    #Plugins
    mariadb_debug("PLUGIN_DIR")

endif(${MARIADB_PUBLIC_VAR_NS}_DEBUG)

########## </debug> ##########
