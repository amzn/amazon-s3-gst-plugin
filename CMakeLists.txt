cmake_minimum_required(VERSION 3.2)
set(CMAKE_CXX_STANDARD 14)
project(amazon-s3-gst-plugin)
set(EXEC_NAME s3sink)
set(VERSION 0.1.0)

message(STATUS "System name is ${CMAKE_SYSTEM_NAME}")
STRING(TOLOWER "${CMAKE_SYSTEM_NAME}" CMAKE_SYSTEM_NAME_LOWER_CASE)
STRING(TOLOWER "${CMAKE_BUILD_TYPE}" CMAKE_BUILD_TYPE_LOWER)

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC")
endif()

find_package(PkgConfig REQUIRED)

set(GSTREAMER_MINIMUM_VERSION 1.0.0)

pkg_check_modules(GST1_TEST gstreamer-1.0)
pkg_check_modules(GST_VIDEO REQUIRED gstreamer-video-1.0)
if (GST1_TEST_FOUND AND NOT ${GST1_TEST_VERSION} VERSION_LESS ${GSTREAMER_MINIMUM_VERSION} )
    pkg_check_modules(GSTREAMER REQUIRED gstreamer-1.0)
    pkg_check_modules(GSTREAMER-BASE REQUIRED gstreamer-base-1.0)
    pkg_check_modules(GSTREAMER-CHECK REQUIRED gstreamer-check-1.0)
    add_definitions(-DGST_API_VERSION_1=1)
else()
    # fallback to gstreamer-0.10
    unset(GSTREAMER_MINIMUM_VERSION)
    pkg_check_modules(GSTREAMER REQUIRED gstreamer-0.10)
    pkg_check_modules(GSTREAMER-BASE REQUIRED gstreamer-base-0.10)
    pkg_check_modules(GSTREAMER-CHECK REQUIRED gstreamer-check-0.10)
endif()

if (DEFINED ENV{AWS_SDK_PATH})
    set(AWS_SDK_PATH "$ENV{AWS_SDK_PATH}")
    set(AWS_SDK_S3_PATH "$ENV{AWS_SDK_PATH}/build/aws-cpp-sdk-s3")
endif ()

set(aws-cpp-sdk-s3_DIR ${AWS_SDK_S3_PATH})
find_package(aws-cpp-sdk-s3)

FILE(WRITE ${CMAKE_CURRENT_SOURCE_DIR}/src/config.h
    "#pragma once\n"
    "#define PACKAGE \"${CMAKE_PROJECT_NAME}\"\n"
    "#define VERSION \"${VERSION}\"\n"
)

include_directories(
    ${GSTREAMER_INCLUDE_DIRS}
    ${GSTREAMER-BASE_INCLUDE_DIRS}
    ${GSTREAMER-CHECK_INCLUDE_DIRS}
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${AWS_SDK_PATH}/aws-cpp-sdk-s3/include
    ${AWS_SDK_PATH}/aws-cpp-sdk-core/include/
    ${AWS_SDK_PATH}/aws-cpp-sdk-sts/include/
)

set(GST_INCLUDE_DIRS
    ${GLIB2_INCLUDE_DIRS}
    ${GST_BASE_INCLUDE_DIRS}
    ${GST_CHECK_INCLUDE_DIRS}
    ${GST_INCLUDE_DIRS}
)

set(GST_LIBRARY_DIRS
    ${GLIB2_LIBRARY_DIRS}
    ${GST_BASE_LIBRARY_DIRS}
    ${GST_CHECK_LIBRARY_DIRS}
    ${GST_LIBRARY_DIRS}
)

set(S3_LIBRARIES
    ${GST_LIBRARIES}
    ${GST_APP_LIBRARIES}
    ${GST_VIDEO_LIBRARIES}
    ${GLIB2_LIBRARIES}
)

set(gstawscredentials_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/src/gstawscredentials.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/gstawscredentials.h
    ${CMAKE_CURRENT_SOURCE_DIR}/src/gstawscredentials.hpp
)

set(S3_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/src/gsts3elements.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/gsts3sink.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/gsts3sink.h
    ${CMAKE_CURRENT_SOURCE_DIR}/src/gsts3uploader.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/gsts3uploader.h
)

set(multipartuploader_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/src/gsts3multipartuploader.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/gsts3multipartuploader.h
)

include_directories(${GST_INCLUDE_DIRS})
link_directories(${GST_LIBRARY_DIRS})

add_library(gstawscredentials STATIC ${gstawscredentials_SOURCES})
target_link_libraries(gstawscredentials ${S3_LIBRARIES})
target_link_libraries(gstawscredentials ${AWS_SDK_PATH}/build/aws-cpp-sdk-core/libaws-cpp-sdk-core.so)
target_link_libraries(gstawscredentials ${AWS_SDK_PATH}/build/aws-cpp-sdk-sts/libaws-cpp-sdk-sts.so)

add_library(multipartuploader STATIC ${multipartuploader_SOURCES})
target_link_libraries(multipartuploader ${S3_LIBRARIES})
target_link_libraries(multipartuploader ${AWS_SDK_PATH}/build/aws-cpp-sdk-core/libaws-cpp-sdk-core.so)
target_link_libraries(multipartuploader ${AWS_SDK_PATH}/build/aws-cpp-sdk-s3/libaws-cpp-sdk-s3.so)

add_library(gsts3elements MODULE ${S3_SOURCES})
target_link_libraries(gsts3elements ${S3_LIBRARIES} gstawscredentials multipartuploader)

file(COPY ${AWS_SDK_PATH}/build/aws-cpp-sdk-core/libaws-cpp-sdk-core.so
    DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
file(COPY ${AWS_SDK_PATH}/build/aws-cpp-sdk-s3/libaws-cpp-sdk-s3.so
    DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
file(COPY ${AWS_SDK_PATH}/build/aws-cpp-sdk-sts/libaws-cpp-sdk-sts.so
    DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
