cmake -B build -S . -DCMAKE_TOOLCHAIN_FILE=/Users/sparelaptop4/vcpkg/scripts/buildsystems/vcpkg.cmake
cmake --build build
docker compose up -d