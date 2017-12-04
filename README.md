# KafkaOffsetTools
This tool can be used to reset the committed offset for kafka
# Build
- install boost(http://www.boost.org), depends on boost_program_options lib; 
- install librdkafka 0.9.4(https://github.com/edenhill/librdkafka/releases/tag/v0.9.4)
- use build.sh to build
# Usage
- ./kafka_offset_tools -h
```
Usage:
  --broker_list arg     kafka broker list
  --topic arg           kafka topic name
  --group arg           consumer group name
  --partition_list arg  reset partiton list
                        "":all parition(default value)
                         Or 1,2,3...
  --reset_pos arg (=0)  reset paritions to position:
                        0:earliest
                        1:latest
```
