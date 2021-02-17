mkdir -p build && cd build

echo "compiling"
cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build .

echo "copying library files"
sudo cp libleveldb.* /usr/local/lib

cd .. && cd include/

sudo cp -R leveldb /usr/local/include
sudo ldconfig

cd .. && cd build
