#ifndef KF_KAFKA_OFFSET_RESETTER_H_
#define KF_KAFKA_OFFSET_RESETTER_H_

#include <set>
#include <string>

//from rdkafka_int.h, avoid to redefine
#define RD_KAFKA_OFFSET_ERROR -1001
#include "rdkafka.h"
//-------------------------------------------------------------
struct rd_kafka_message_s;

namespace kt {

class KtKafkaOffsetResetter {
    public:
        struct PartitionWaterMark {
            int64_t lo;
            int64_t hi;
        };

    public:
        KtKafkaOffsetResetter(const std::string& broker_list,
                    const std::string& topic,
                    const std::string& group);
        ~KtKafkaOffsetResetter();

    public:
        bool init();
        void uninit() {
            if (NULL != rd_kafka_handle_) {
                rd_kafka_destroy(rd_kafka_handle_);
                rd_kafka_handle_ = NULL;
            }
        }

        bool getTopicPartitionList(const std::string& topic, std::set<int32_t>& partitions);
        bool getTopicPartitionWaterMark(const std::string& topic,
                    int32_t partition,
                    PartitionWaterMark& water_mark);
        void resetByKF(std::set<int32_t> partitions,
                    int reset_offset_strategy);
        bool checkGroupActive(const std::string& topic,
                    const std::string& group);

    private:
        bool initRdKafkaHandle();
        bool setRdKafkaConfig(rd_kafka_conf_t* rd_kafka_conf,
                    const char* item,
                    const char* value);
        static void rdkafka_rebalance_cb(rd_kafka_t *rk,
            rd_kafka_resp_err_t err,
            rd_kafka_topic_partition_list_t *partitions,
            void *opaque);

    private:
        rd_kafka_conf_t* rd_kafka_conf_;
        rd_kafka_t* rd_kafka_handle_;
        rd_kafka_topic_partition_list_t* rd_kafka_topic_list_;

        std::string broker_list_;
        std::string topic_;
        std::string group_;

        bool rebalance_done_;
};

} //namespace kt
#endif //#ifndef KF_KAFKA_OFFSET_RESETTER_H_
