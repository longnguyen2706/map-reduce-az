#!/usr/bin/env bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
numprocs=$(cat /proc/cpuinfo | grep processor | wc -l)

sudo apt-get install -y g++ git libboost-all-dev libwebsocketpp-dev openssl libssl-dev ninja-build libxml2-dev uuid-dev libunittest++-dev libcurl4-openssl-dev 

echo "Creating the CMake Find file"
cd /tmp
wget https://raw.githubusercontent.com/Azure/azure-storage-cpp/master/Microsoft.WindowsAzure.Storage/cmake/Modules/LibFindMacros.cmake
sudo mv LibFindMacros.cmake /usr/local/share/cmake-3.19/Modules
wget https://raw.githubusercontent.com/Tokutek/mongo/master/cmake/FindSSL.cmake
sudo mv FindSSL.cmake /usr/local/share/cmake-3.19/Modules

echo "Install NEW Azure Storage CPP"
cd ~/src
git clone https://github.com/Azure/azure-sdk-for-cpp/
cd azure-sdk-for-cpp/
mkdir -p build/
cd build/
cmake .. -DCMAKE_BUILD_TYPE=Debug -DBUILD_SHARED_LIBS=on
cmake --build .
sudo make install

cd ..
mkdir -p build-static/
cd build-static/
cmake .. -DCMAKE_BUILD_TYPE=Debug -DBUILD_STATIC_LIBS=on
cmake --build .
sudo make install
sudo ldconfig
