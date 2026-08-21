cmake_minimum_required(VERSION 3.10)

SET(PROJECT_NAME_STR HERCULES)
SET(CMAKE_CXX_STANDARD 17)
SET(CMAKE_CXX_STANDARD_REQUIRED ON)
SET(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_CURRENT_SOURCE_DIR}/CMake)

PROJECT (${PROJECT_NAME_STR} C CXX)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug CACHE STRING "Choose the type of build." FORCE)
endif()

# set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC")
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# ===================
#  SANITIZERS CONFIGURATION
option(USE_ASAN "Enable AddressSanitizer (Memory errors/SegFault)" OFF)
option(USE_TSAN "Enable ThreadSanitizer (Data races)" OFF)

if(USE_ASAN AND USE_TSAN)
    message(FATAL_ERROR "You cannot enable ASan and TSan at the same time. Choose one.")
endif()

if(USE_ASAN)
    message(STATUS "Enabling AddressSanitizer (ASan)...")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
elseif(USE_TSAN)
    message(STATUS "Enabling ThreadSanitizer (TSAN)...")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=thread")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread")
endif()
# ===================

find_package(PCRE)

include(FindPkgConfig)
pkg_check_modules(GLIB glib-2.0 REQUIRED)
include_directories(${GLIB_INCLUDE_DIRS})

LINK_DIRECTORIES(${GLIB_LIBRARY_DIRS})

# find_package(Jemalloc)
# set(WITH_JEMALLOC_DEFAULT   ${JEMALLOC_FOUND})

# Set CMake to enable debug symbols and disable optimizations
# set(CMAKE_BUILD_TYPE Debug)


FIND_PACKAGE(Threads REQUIRED)

find_package(UCX REQUIRED COMPONENTS ucp uct ucs)
INCLUDE_DIRECTORIES( ${UCX_INCLUDE_DIRS} )

INCLUDE_DIRECTORIES( "${PROJECT_SOURCE_DIR}/include" )
LINK_DIRECTORIES("${PROJECT_SOURCE_DIR}/build/app")

FILE(GLOB HEADERS "include/*")
SOURCE_GROUP(include FILES ${HEADERS})
SET(CMAKE_SUPPRESS_REGENERATION TRUE)

SET(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}")


if (CMAKE_BUILD_TYPE STREQUAL "Debug" OR CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
    add_compile_definitions(__DEBUG__)
endif ()

# find_package(FUSE)

ADD_SUBDIRECTORY(app)

# if(FUSE_FOUND)
#    ADD_SUBDIRECTORY(fuse)
# endif(FUSE_FOUND)

ADD_SUBDIRECTORY(tools)
