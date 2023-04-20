FROM ubuntu:20.04
RUN apt-get update
RUN apt-get install -y ca-certificates
RUN DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get -y install tzdata
RUN apt-get install -y g++ libboost-all-dev ant check build-essential autoconf libtool pkg-config checkinstall git zlib1g libssl-dev
RUN apt-get install -y python3-pip
RUN python3 -m pip install bottle etcd3==0.5.0 requests
RUN apt-get install iputils-ping -y
RUN apt-get install libwebsocketpp-dev openssl libssl-dev ninja-build libxml2-dev uuid-dev libunittest++-dev -y

RUN apt-get install wget sudo curl -y
RUN apt-get install cmake -y
RUN apt-get install libgtest-dev \
            libgoogle-glog-dev \
            libcpprest-dev \
            libgrpc* \
            libprotobuf-dev \
            libprotoc-dev \
            protobuf-compiler-grpc \
            -y

RUN mkdir -p ~/src
RUN cd ~/src  && \
    git clone https://github.com/etcd-cpp-apiv3/etcd-cpp-apiv3.git  && \
    cd etcd-cpp-apiv3  && \
    mkdir build && cd build  && \
    cmake ..  && \
    make -j2 && sudo make install  

# Following https://devblogs.microsoft.com/azure-sdk/intro-cpp-sdk/ 
# to install azure blob storage via vcpkg
# INSTALL AZURE BLOB STORAGE APIs
RUN apt-get install -y curl zip unzip tar
RUN git clone https://github.com/microsoft/vcpkg
RUN ./vcpkg/bootstrap-vcpkg.sh
RUN ./vcpkg/vcpkg install --clean-after-build \
        azure-storage-blobs-cpp \
        azure-storage-files-shares-cpp
        # azure-storage-cpp # not download old version
RUN apt-get install nlohmann-json3-dev
RUN echo "export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH" >> ~/.bashrc
