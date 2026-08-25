# System dependent compiler flags.
#
# Everything here is a compile *option*, applied with ADD_COMPILE_OPTIONS.
# ADD_DEFINITIONS is for preprocessor defines; bare flags passed through it
# happen to work but are the wrong tool, and the mistake is easy to copy.

INCLUDE(CheckCCompilerFlag)

IF (WIN32 OR NOT UNIX)
    RETURN()
ENDIF()

ADD_COMPILE_DEFINITIONS(_GNU_SOURCE)

# A loadable module has to be position independent. CMake already does this
# for MODULE targets; stating it means a stray OBJECT or static target added
# later does not quietly get it wrong.
SET(CMAKE_POSITION_INDEPENDENT_CODE ON)

ADD_COMPILE_OPTIONS(
    -Wall
    -Wextra
    -Wreturn-type
    -Wstrict-prototypes
    -Wmissing-prototypes
    -Wmissing-declarations
    -Wpointer-arith
    -Wchar-subscripts
    -Wformat=2
    -Wbad-function-cast
    -Wshadow
    -Wuninitialized
)

# -fstack-protector-strong where the compiler has it, plain -fstack-protector
# otherwise. The original asked for both unconditionally, which is just the
# weaker one being overridden.
CHECK_C_COMPILER_FLAG("-fstack-protector-strong" WITH_STACK_PROTECTOR_STRONG)
IF (WITH_STACK_PROTECTOR_STRONG)
    ADD_COMPILE_OPTIONS(-fstack-protector-strong)
ELSE()
    CHECK_C_COMPILER_FLAG("-fstack-protector" WITH_STACK_PROTECTOR)
    IF (WITH_STACK_PROTECTOR)
        ADD_COMPILE_OPTIONS(-fstack-protector)
    ENDIF()
ENDIF()

# _FORTIFY_SOURCE is a glibc feature selected by a preprocessor define, not a
# compiler flag -- CHECK_C_COMPILER_FLAG on it tests nothing, since every
# compiler accepts every -D. It also needs optimisation to do anything: at -O0
# glibc emits a #warning and disables itself. So it goes only on the optimised
# configurations, and -U comes first because some toolchains predefine it.
ADD_COMPILE_OPTIONS(
    "$<$<OR:$<CONFIG:Release>,$<CONFIG:RelWithDebInfo>,$<CONFIG:MinSizeRel>>:-U_FORTIFY_SOURCE;-D_FORTIFY_SOURCE=2>"
)

# Readable stack traces when something crashes the server under a debugger
ADD_COMPILE_OPTIONS("$<$<CONFIG:Debug>:-fno-omit-frame-pointer>")

# Per configuration optimisation and debug flags are left to CMake, which
# already supplies -O3 -DNDEBUG for Release, -O2 -g -DNDEBUG for RelWithDebInfo
# and -g for Debug. Setting them again here only lets the two disagree.
