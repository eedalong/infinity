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

#include <chrono>
#include <vector>
#include <iostream>
#include <algorithm>

#include <infinity/core/Context.h>
#include <infinity/memory/Buffer.h>
#include <infinity/memory/RegionToken.h>
#include <infinity/queues/QueuePair.h>
#include <infinity/queues/QueuePairFactory.h>
#include <infinity/requests/RequestToken.h>

#define PORT_NUMBER 3344
#define SERVER_IP "155.198.152.17"

#define NODE_COUNT 10000
#define FEATURE_DIM 128
#define FEATURE_TYPE_SIZE 4
#define TEST_COUNT 35000
#define MAX_OUTSTANDING_REQ 1
#define POST_LIST_SIZE 32
#define CQ_MOD 25
#define QP_NUM 4

int min(int a, int b){
    if(a < b){
        return a;
    }
    return b;
}


uint64_t timeDiff(struct timeval stop, struct timeval start) {
  return (stop.tv_sec * 1000000L + stop.tv_usec) -
         (start.tv_sec * 1000000L + start.tv_usec);
}

// Usage: ./progam -s for server and ./program for client component
int main(int argc, char **argv) {

  bool isServer = false;
  bool random = true;
  bool sort_index = false;

  while (argc > 1) {
    if (argv[1][0] == '-') {
      switch (argv[1][1]) {
        case 's': {
          isServer = true;
          break;
        }
        case 'l': {
          random = false;
          break;
        }
        case 't': {
          sort_index = false;
          break;
        }
      }
    }
    ++argv;
    --argc;
  }
  if(random){
    printf("Test Random Data Access \n");
  }else{
    printf("Test Sequential Data Access \n");
  }
  if(sort_index){
    printf("Test Data Access With TLB Optimization\n");
  }
  std::vector<infinity::queues::QueuePair*> qps;
  infinity::core::Context *context = new infinity::core::Context();
  infinity::queues::QueuePairFactory *qpFactory =
      new infinity::queues::QueuePairFactory(context);

  qps.resize(QP_NUM);

  if (isServer) {

    printf("Creating buffers to read from and write to\n");
    std::cout << "Server Buffer Size " << NODE_COUNT * FEATURE_DIM * FEATURE_TYPE_SIZE << std::endl;
    infinity::memory::Buffer *bufferToReadWrite =
        new infinity::memory::Buffer(context, NODE_COUNT * FEATURE_DIM * FEATURE_TYPE_SIZE);
    infinity::memory::RegionToken *bufferToken =
        bufferToReadWrite->createRegionToken();
    
    printf("Creating buffers to receive a message\n");
		infinity::memory::Buffer *bufferToReceive = new infinity::memory::Buffer(context, 128 * sizeof(char));
		context->postReceiveBuffer(bufferToReceive);


    printf("Setting up connection (blocking)\n");
    qpFactory->bindToPort(PORT_NUMBER);
    for(int qp_index=0; qp_index < QP_NUM; qp_index++){
        qps[qp_index] = qpFactory->acceptIncomingConnection(bufferToken, sizeof(infinity::memory::RegionToken));
    }
    printf("Waiting for message (blocking)\n");
    infinity::core::receive_element_t receiveElement;
    while (!context->receive(&receiveElement))
        ;

    printf("Message received\n");
    delete bufferToReadWrite;
    delete bufferToReceive;

  } else {

    std::vector<uint64_t> local_offsets(POST_LIST_SIZE, 0);
    std::vector<uint64_t> remote_offsets(POST_LIST_SIZE, 0);
    int start_request = 0;
    int end_request = 0;
    infinity::queues::SendRequestBuffer send_buffer(POST_LIST_SIZE);

    printf("Connecting to remote node\n");
    std::vector<infinity::memory::RegionToken *> remoteBufferTokens;
    remoteBufferTokens.resize(QP_NUM);
    for(int qp_index = 0; qp_index < QP_NUM; qp_index ++){
        qps[qp_index] = qpFactory->connectToRemoteHost(SERVER_IP, PORT_NUMBER);
        remoteBufferTokens[qp_index] = (infinity::memory::RegionToken *)qps[qp_index]->getUserData();
    }

    printf("Creating buffers\n");
    std::vector<infinity::memory::Buffer *> buffers;
    infinity::memory::Buffer *buffer1Sided =
        new infinity::memory::Buffer(context, NODE_COUNT * FEATURE_DIM * FEATURE_TYPE_SIZE);
    infinity::memory::Buffer *buffer2Sided = new infinity::memory::Buffer(context, 128 * sizeof(char));


    printf("Reading content from remote buffer\n");
    infinity::requests::RequestToken requestToken(context);

    // warm up

    printf("Warm up\n");
    for (int k = 0; k < 10; k++) {
      int request_node = rand() % NODE_COUNT;
      uint64_t offset = request_node * FEATURE_DIM * FEATURE_TYPE_SIZE;
      //std::cout << "Getting Data From " << offset << " To " << offset + FEATURE_DIM * FEATURE_TYPE_SIZE << std::endl;
      qps[k % QP_NUM]->read(buffer1Sided, 0, remoteBufferTokens[k % QP_NUM], offset, FEATURE_DIM * FEATURE_TYPE_SIZE,
                infinity::queues::OperationFlags(), &requestToken);
      requestToken.waitUntilCompleted();
    }

    printf("Start Real Test \n");
    auto start = std::chrono::system_clock::now();
    int avaliable = MAX_OUTSTANDING_REQ;
    for (int k = 0; k < TEST_COUNT; k++) {
        for(int multi_read_index = 0; multi_read_index < POST_LIST_SIZE; multi_read_index ++){
            int request_node = (k + multi_read_index) % NODE_COUNT;
            if(random){
                request_node = rand() % NODE_COUNT;
            }
            uint64_t remote_node_offset = request_node * FEATURE_DIM * FEATURE_TYPE_SIZE;
            local_offsets[multi_read_index] = request_node * FEATURE_DIM * FEATURE_TYPE_SIZE;
            remote_offsets[multi_read_index] = remote_node_offset;
        }
        if(sort_index){
          std::sort(local_offsets.begin(), local_offsets.end());
          std::sort(remote_offsets.begin(), remote_offsets.end());
        }

        if(k % CQ_MOD ==  CQ_MOD -1){
            qps[k % QP_NUM]->multiRead(buffer1Sided, local_offsets, remoteBufferTokens[k % QP_NUM], remote_offsets, FEATURE_DIM * FEATURE_TYPE_SIZE,
                        infinity::queues::OperationFlags(), &requestToken, send_buffer);
            requestToken.waitUntilCompleted();
        }else{
            qps[k % QP_NUM]->multiRead(buffer1Sided, local_offsets, remoteBufferTokens[k % QP_NUM], remote_offsets, FEATURE_DIM * FEATURE_TYPE_SIZE,
                        infinity::queues::OperationFlags(), nullptr, send_buffer);
        }
    }

    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> diff = end - start;
    printf("Avg Bandwidth is %f MB/s\n", (POST_LIST_SIZE * TEST_COUNT *  FEATURE_DIM/ (1024.0 * 1024.0 ) ) * FEATURE_TYPE_SIZE / diff.count() );

    printf("Sending message to remote host from QueuePair %d\n", QP_NUM-1);
    qps[QP_NUM-1]->send(buffer2Sided, &requestToken);
    requestToken.waitUntilCompleted();


    delete buffer1Sided;
    delete buffer2Sided;
  }

  for(int index = 0; index < QP_NUM; index++){
    delete qps[index];
  }
  delete qpFactory;
  delete context;

  return 0;
}