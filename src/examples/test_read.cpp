/**
 * Examples - Read/Write/Send Operations
 *
 * (c) 2018 Claude Barthels, ETH Zurich
 * Contact: claudeb@inf.ethz.ch
 *
 */

#include <cassert>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include <vector>

#include <infinity/core/Context.h>
#include <infinity/memory/Buffer.h>
#include <infinity/memory/RegionToken.h>
#include <infinity/queues/QueuePair.h>
#include <infinity/queues/QueuePairFactory.h>
#include <infinity/requests/RequestToken.h>

#define PORT_NUMBER 3344
#define SERVER_IP "155.198.152.17"

uint64_t timeDiff(struct timeval stop, struct timeval start) {
  return (stop.tv_sec * 1000000L + stop.tv_usec) -
         (start.tv_sec * 1000000L + start.tv_usec);
}

// Usage: ./progam -s for server and ./program for client component
int main(int argc, char **argv) {

  bool isServer = false;

  while (argc > 1) {
    if (argv[1][0] == '-') {
      switch (argv[1][1]) {

      case 's': {
        isServer = true;
        break;
      }
      }
    }
    ++argv;
    --argc;
  }

  infinity::core::Context *context = new infinity::core::Context();
  infinity::queues::QueuePairFactory *qpFactory =
      new infinity::queues::QueuePairFactory(context);
  infinity::queues::QueuePair *qp;

  if (isServer) {

    printf("Creating buffers to read from and write to\n");
    infinity::memory::Buffer *bufferToReadWrite =
        new infinity::memory::Buffer(context, 512 * sizeof(char));
    infinity::memory::RegionToken *bufferToken =
        bufferToReadWrite->createRegionToken();

    printf("Creating buffers to receive a message\n");
    infinity::memory::Buffer *bufferToReceive =
        new infinity::memory::Buffer(context, 128 * sizeof(char));
    context->postReceiveBuffer(bufferToReceive);

    printf("Setting up connection (blocking)\n");
    qpFactory->bindToPort(PORT_NUMBER);
    qp = qpFactory->acceptIncomingConnection(
        bufferToken, sizeof(infinity::memory::RegionToken));

    printf("Waiting for message (blocking)\n");
    infinity::core::receive_element_t receiveElement;
    while (!context->receive(&receiveElement))
      ;

    printf("Message received\n");
    delete bufferToReadWrite;
    delete bufferToReceive;

  } else {

    printf("Connecting to remote node\n");
    qp = qpFactory->connectToRemoteHost(SERVER_IP, PORT_NUMBER);
    infinity::memory::RegionToken *remoteBufferToken =
        (infinity::memory::RegionToken *)qp->getUserData();

    printf("Creating buffers\n");
    std::vector<infinity::memory::Buffer *> buffers;
    infinity::memory::Buffer *buffer1Sided =
        new infinity::memory::Buffer(context, (size_t)512 * sizeof(char));

    printf("Reading content from remote buffer\n");
    std::vector<infinity::requests::RequestToken *> requests;
    for (int i = 0; i < 1000; i++) {
      requests.push_back(new infinity::requests::RequestToken(context));
      printf("reading %d", i);
    }
    uint64_t cnt = 0;
    for (int i = 0; i < 100; i++) {
      struct timeval start;
      struct timeval stop;
      uint64_t cur = 0;
      for (int j = 0; j < 100; j++) {
        gettimeofday(&start, NULL);
        for (int k = 0; k < 1000; k++) {
          qp->read(buffer1Sided, 0, remoteBufferToken, 0, 512,
                   infinity::queues::OperationFlags(), requests[k]);
        }
        gettimeofday(&stop, NULL);
        cur += timeDiff(stop, start);
		for (int k = 0; k < 1000; k++) {
			requests[k]->waitUntilCompleted();
		}
      }

      printf("Cur 50MB took %lu\n", cur);
      cnt += cur;
    }

    printf("Avg 50MB took %lu\n", cnt / 100);

    // printf("Writing content to remote buffer\n");
    // qp->write(buffer1Sided, remoteBufferToken, &requestToken);
    // requestToken.waitUntilCompleted();

    printf("Sending message to remote host\n");
    infinity::requests::RequestToken requestToken(context);
    infinity::memory::Buffer *buffer2Sided =
        new infinity::memory::Buffer(context, 128 * sizeof(char));
    qp->send(buffer2Sided, &requestToken);
    requestToken.waitUntilCompleted();

    delete buffer1Sided;
    delete buffer2Sided;
  }

  delete qp;
  delete qpFactory;
  delete context;

  return 0;
}
