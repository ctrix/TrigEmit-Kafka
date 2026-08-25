#
# Derive the project version from git. The tag is the single source of
# truth: there is no version number written down anywhere else.
#
# Two values come out of this, and they answer different questions.
#
#   GIT_VERSION     the numeric version, from the nearest reachable tag with
#                   any leading "v" stripped. This is what PROJECT(VERSION)
#                   takes, so it must be digits and dots or nothing else.
#                   Falls back to 0.0.0 when there is no usable tag.
#
#   GIT_COMMIT_ID   the full build identifier, as 'git describe' reports it:
#                   the tag when the build sits exactly on one, otherwise the
#                   tag plus commit distance plus short hash, plus "-dirty"
#                   when the working tree was modified. With no tags at all
#                   this is just the short hash, and with no git information
#                   whatsoever it is the literal "unknown".
#
# Neither is ever empty; both are compiled into config.h.
#
# Tag a release with 'git tag v1.2.3' (and 'git push --tags').
#

SET(GIT_VERSION "0.0.0")
SET(GIT_COMMIT_ID "unknown")

FIND_PACKAGE(Git QUIET)

IF(NOT GIT_FOUND)
    MESSAGE(STATUS "Git not found, version information unavailable")
    RETURN()
ENDIF()

# --tags   also consider lightweight tags
# --always fall back to a bare commit hash when no tag is reachable
# --dirty  mark builds made from a modified working tree
EXECUTE_PROCESS(
    COMMAND ${GIT_EXECUTABLE} describe --tags --always --dirty
    WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}/..
    RESULT_VARIABLE res_var
    OUTPUT_VARIABLE git_id
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
)

IF(NOT res_var EQUAL 0 OR "${git_id}" STREQUAL "")
    # Reached when git is present but has nothing to describe, which is the
    # normal state of a repository before its first commit.
    MESSAGE(STATUS "No git version information available (no commits or not a repository)")
    RETURN()
ENDIF()

SET(GIT_COMMIT_ID "${git_id}")

# --abbrev=0 gives the nearest tag on its own, with no distance or hash
EXECUTE_PROCESS(
    COMMAND ${GIT_EXECUTABLE} describe --tags --abbrev=0
    WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}/..
    RESULT_VARIABLE res_var
    OUTPUT_VARIABLE git_tag
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
)

IF(NOT res_var EQUAL 0 OR "${git_tag}" STREQUAL "")
    MESSAGE(STATUS "No tag reachable, version defaults to ${GIT_VERSION}")
    RETURN()
ENDIF()

STRING(REGEX REPLACE "^[vV]" "" git_tag_version "${git_tag}")

# PROJECT(VERSION) rejects anything that is not digits separated by dots, so
# a tag naming something else must not reach it.
IF(git_tag_version MATCHES "^[0-9]+(\\.[0-9]+)*$")
    SET(GIT_VERSION "${git_tag_version}")
ELSE()
    MESSAGE(STATUS "Tag '${git_tag}' is not a version number, version defaults to ${GIT_VERSION}")
ENDIF()
