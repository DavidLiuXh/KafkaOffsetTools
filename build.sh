#!/bin/bash

gcc kafka_offset_tools.cc kafka_offset_resetter.cc -I/home/test_user_2/works/qbus_2-0/client/cxx/thirdparts/librdkafka/src -Wl,-dn  -lboost_program_options -L/usr/local/lib -lrdkafka -Wl,-dy  -lpthread -lm -lstdc++ -lrt -lz  -o kafka_offset_tools
