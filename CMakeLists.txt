cmake_minimum_required(VERSION 3.2)

project(cmake-skeleton)
set(TARGET foobar)

include_directories(sources includes)
file(GLOB SOURCE_FILES "sources/*.cpp" "includes/*.h")

add_executable(${TARGET} ${SOURCE_FILES})
