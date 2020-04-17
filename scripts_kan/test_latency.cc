#include <iostream>
#include <unistd.h>
#include <chrono>
#include <assert.h>
#include <fcntl.h>
#include <unistd.h>

using namespace std;


int main() {

  std::cout << "Start to test single thread IO\n";

  int fd = open("/mnt/optane/test_file", O_RDONLY | O_DIRECT);
  int io_size = 4096;
  char * read_buf = (char *) malloc(sizeof(char) * io_size);
  int ret = posix_memalign((void **)&read_buf, 512, io_size);
  long long avg_lat = 0;

  for (int i = 0; i < 10000; i ++) {
    auto timeStart = std::chrono::high_resolution_clock::now();
    int sz = pread(fd, read_buf, io_size, i*io_size);
    assert(sz == io_size);
    auto timeEnd = std::chrono::high_resolution_clock::now();
    long long duration = std::chrono::duration_cast<std::chrono::microseconds>(timeEnd - timeStart).count();
    std::cout << "last lat" << duration << "us\n"; 
    avg_lat += duration;
    usleep(1000);
    //for (int j = 0; j < 100000; j++);
  }

  std::cout << "Average latency: " << avg_lat / 10000 << std::endl;
  return 0;
}
