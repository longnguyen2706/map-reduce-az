project(MapReduce)
cmake_minimum_required(VERSION 3.1)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${PROJECT_SOURCE_DIR}/CMakeModules/")

set(Boost_USE_STATIC_LIBS ON)
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_RUNTIME OFF)
find_package(Boost REQUIRED COMPONENTS log thread system filesystem program_options)
find_package(SSL REQUIRED)
find_package(nlohmann_json REQUIRED)

message(STATUS "Using Boost ${Boost_VERSION}")

# Check if the vcpkg directory exists
# Include the vcpkg toolchain file
# https://stackoverflow.com/questions/55496611/cmake-cannot-find-libraries-installed-with-vcpkg
if(EXISTS "/vcpkg/scripts/buildsystems/vcpkg.cmake")
  message(WARNING "use/vcpkg for azure deps")
  include(/vcpkg/scripts/buildsystems/vcpkg.cmake)
endif()

find_package(azure-storage-blobs-cpp CONFIG REQUIRED)
find_package(azure-storage-files-shares-cpp CONFIG REQUIRED)
add_definitions(-std=c++17)

include(FindProtobuf)
find_package(Protobuf REQUIRED)
find_package(Threads REQUIRED)
find_package(Boost REQUIRED COMPONENTS system thread random)

find_package(Check REQUIRED)
find_package(cpprestsdk REQUIRED)
find_package(etcd-cpp-api REQUIRED)

# find_package(Protobuf CONFIG REQUIRED) # TODO: fix this 
message(STATUS "Using protobuf ${Protobuf_VERSION}")

find_package(PkgConfig REQUIRED)
pkg_check_modules(glog REQUIRED libglog)
# find_package(glog REQUIRED)

set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
set(_REFLECTION gRPC::grpc++_reflection)
set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)
# set(_GLOG glog::glog)

# Find gRPC installation
# Looks for gRPCConfig.cmake file installed by gRPC's cmake installation.
# find_package(gRPC CONFIG REQUIRED) # TODO fix this
find_package(gRPC QUIET)
message(STATUS "Using gRPC ${gRPC_VERSION}")

set(_GRPC_GRPCPP gRPC::grpc++)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)

include_directories(/usr/local/include)
link_directories(/usr/local/lib)

message(STATUS ${CMAKE_SOURCE_DIR})
set(PROTO_PATH "${CMAKE_SOURCE_DIR}/protos")
set(MAPREDUCE_PROTO "${CMAKE_SOURCE_DIR}/protos/mrmsg.proto")
set(GENERATED_PROTOBUF_PATH "${CMAKE_BINARY_DIR}/generated")
file(MAKE_DIRECTORY ${GENERATED_PROTOBUF_PATH})

set(MAPREDUCE_PB_CPP_FILE "${GENERATED_PROTOBUF_PATH}/mrmsg.pb.cc")
set(MAPREDUCE_PB_H_FILE "${GENERATED_PROTOBUF_PATH}/mrmsg.pb.h")
set(MAPREDUCE_GRPC_PB_CPP_FILE "${GENERATED_PROTOBUF_PATH}/mrmsg.grpc.pb.cc")
set(MAPREDUCE_GRPC_PB_H_FILE "${GENERATED_PROTOBUF_PATH}/mrmsg.grpc.pb.h")

add_custom_command(
    OUTPUT "${MAPREDUCE_PB_H_FILE}"
           "${MAPREDUCE_PB_CPP_FILE}"
           "${MAPREDUCE_GRPC_PB_H_FILE}"
           "${MAPREDUCE_GRPC_PB_CPP_FILE}"
    COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
    ARGS "--proto_path=${PROTO_PATH}"
         "--cpp_out=${GENERATED_PROTOBUF_PATH}"
         "${MAPREDUCE_PROTO}"
    COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
    ARGS "--proto_path=${PROTO_PATH}"
         "--grpc_out=${GENERATED_PROTOBUF_PATH}"
         "--plugin=protoc-gen-grpc=/usr/bin/grpc_cpp_plugin" # THIS Was change due to the fact that the grpc_cpp_plugin is not in the path
         "${MAPREDUCE_PROTO}"
    )


# Add protobuf library
add_library(hw_grpc_proto ${MAPREDUCE_PB_H_FILE} ${MAPREDUCE_PB_CPP_FILE} ${MAPREDUCE_GRPC_PB_H_FILE} ${MAPREDUCE_GRPC_PB_CPP_FILE})
target_link_libraries(hw_grpc_proto ${_REFLECTION} ${_GRPC_GRPCPP} ${_PROTOBUF_LIBPROTOBUF})

set(GENERATED_PROTOBUF_FILES ${MAPREDUCE_PB_H_FILE} ${MAPREDUCE_PB_CPP_FILE}
    ${MAPREDUCE_GRPC_PB_H_FILE} ${MAPREDUCE_GRPC_PB_CPP_FILE})

include_directories(${GENERATED_PROTOBUF_PATH})
include_directories(${CHECK_INCLUDE_DIRS})
include_directories(${etcd-cpp-api_INCLUDE_DIRS})
include_directories(${AZURE_STORAGE_INCLUDE_DIR}/azure-sdk-for-cpp)
   
link_libraries(grpc++_unsecure grpc gpr ${PROTOBUF_LIBRARY})

add_executable(master src/master/master.cc )
target_link_libraries(
        master 
        hw_grpc_proto 
        ${_REFLECTION} 
        etcd-cpp-api
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIBPROTOBUF}
        ${Boost_LIBRARIES}
        ${SSL_LIBRARIES}
        Azure::azure-storage-blobs
        Azure::azure-storage-files-shares
        nlohmann_json::nlohmann_json
    )

add_executable(worker src/worker/worker.cc )
target_link_libraries(worker hw_grpc_proto 
        ${_REFLECTION}
        etcd-cpp-api
        ${_GRPC_GRPCPP}
        ${_PROTOBUF_LIBPROTOBUF} 
        ${glog_LIBRARIES}
        ${Boost_LIBRARIES}
        ${SSL_LIBRARIES}
        Azure::azure-storage-blobs
        Azure::azure-storage-files-shares
    )

add_executable(proxy src/proxy/proxy.cc )
    target_link_libraries(
            proxy
            hw_grpc_proto 
            ${_REFLECTION} 
            etcd-cpp-api
            ${_GRPC_GRPCPP}
            ${_PROTOBUF_LIBPROTOBUF}
            ${Boost_LIBRARIES}
            ${SSL_LIBRARIES}
            Azure::azure-storage-blobs
            Azure::azure-storage-files-shares
            nlohmann_json::nlohmann_json
        )
