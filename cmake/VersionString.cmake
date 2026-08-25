#
# Determine a version string for this build.
#
# Preference order:
#   1. the most recent git tag, as reported by 'git describe'
#      (create one with 'git tag <name>' and 'git push --tags')
#   2. the short commit hash, when the repository has no tags yet
#   3. the literal "unknown", when there is no usable git information
#      at all: no git binary, no repository, or a repository without
#      any commit.
#
# The result is exported as GIT_COMMIT_ID and must never be empty, as
# it is substituted into config.h and compiled into the module.
#

FIND_PACKAGE(Git QUIET)

SET(GIT_COMMIT_ID "unknown")

IF(GIT_FOUND)
    # --tags   also consider lightweight tags
    # --always fall back to a bare commit hash when no tag is reachable
    # --dirty  mark builds made from a modified working tree
    EXECUTE_PROCESS(
        COMMAND ${GIT_EXECUTABLE} describe --tags --always --dirty
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        RESULT_VARIABLE res_var
        OUTPUT_VARIABLE git_id
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
    )

    IF(res_var EQUAL 0 AND NOT "${git_id}" STREQUAL "")
        SET(GIT_COMMIT_ID "${git_id}")
    ELSE()
        # Reached when git is present but has nothing to describe, which
        # is the normal state of a repository before its first commit.
        MESSAGE(STATUS "No git version information available (no commits or not a repository)")
    ENDIF()
ELSE()
    MESSAGE(STATUS "Git not found")
ENDIF()

MESSAGE(STATUS "GIT_COMMIT_ID: ${GIT_COMMIT_ID}")
