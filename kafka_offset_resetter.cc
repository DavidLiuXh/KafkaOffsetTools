#include "kafka_offset_resetter.h"

#include <string.h>
#include <arpa/inet.h>

#include <iostream>
//------------------------------------------------------------
namespace kt {

KtKafkaOffsetResetter::KtKafkaOffsetResetter(const std::string& broker_list,
            const std::string& topic,
            const std::string& group):
    rd_kafka_conf_(NULL),
    rd_kafka_handle_(NULL),
    rd_kafka_topic_list_(NULL),
    broker_list_(broker_list),
    topic_(topic),
    group_(group),
    rebalance_done_(false) {
}

KtKafkaOffsetResetter::~KtKafkaOffsetResetter() {
}

bool KtKafkaOffsetResetter::init() {
    return (!broker_list_.empty() &&
                initRdKafkaHandle());
} 

bool KtKafkaOffsetResetter::initRdKafkaHandle() {
    bool rt = false;

    if (NULL != rd_kafka_handle_) {
        rt = true;
    } else {
        rd_kafka_conf_ = rd_kafka_conf_new();
        if (NULL != rd_kafka_conf_) {
            rt = setRdKafkaConfig(rd_kafka_conf_,
                        "group.id",
                        group_.c_str());
            if (rt) {
                rd_kafka_conf_set_opaque(rd_kafka_conf_, static_cast<void*>(this));
                rd_kafka_conf_set_rebalance_cb(rd_kafka_conf_, &KtKafkaOffsetResetter::rdkafka_rebalance_cb);
                char err_str[512] = {0};
                rd_kafka_handle_ = rd_kafka_new(RD_KAFKA_CONSUMER,
                            rd_kafka_conf_,
                            err_str,
                            sizeof(err_str));
                if (NULL == rd_kafka_handle_) {
                    std::cout << __FUNCTION__ << "Failed to create new consumer | error msg:" << err_str << std::endl;
                    rt = false;
                } else if (0 == rd_kafka_brokers_add(rd_kafka_handle_, broker_list_.c_str())) {
                    std::cout << __FUNCTION__ << " | Failed to rd_kafka_broker_add | broker list:" << broker_list_ << std::endl;;
                    rt = false;
                } else {
                    rt = true;
                }
            } else {
                std::cout << __FUNCTION__ << " | Failed to set group: " << group_ << std::endl;
            }
        } else {
            std::cout << __FUNCTION__ << " | Failed to rd_kafka_conf_new " << std::endl;
        }
    }

    return rt;
}

bool KtKafkaOffsetResetter::getTopicPartitionList(const std::string& topic,
            std::set<int32_t>& partitions) {
    partitions.clear();

    if (!topic.empty() &&
                initRdKafkaHandle()) {
        rd_kafka_topic_t* rd_kafka_topic = rd_kafka_topic_new(rd_kafka_handle_,
                    topic.c_str(),
                    NULL);
        if (NULL != rd_kafka_topic) {
            int retry = 0;
RETRY:
            const struct rd_kafka_metadata *metadata;
            rd_kafka_resp_err_t err = rd_kafka_metadata(rd_kafka_handle_,
                        0,
                        rd_kafka_topic,
                        &metadata,
                        500);
            if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                if (++retry < 4) {
                    goto RETRY;
                }
            }

            if (err == RD_KAFKA_RESP_ERR_NO_ERROR) {
                if (metadata->topic_cnt > 0) {
                    const struct rd_kafka_metadata_topic *t = &metadata->topics[0];
                    if (NULL != t) {
                        if (t->partition_cnt > 0) {
                            for (int j = 0 ; j < t->partition_cnt ; ++j) {
                                const struct rd_kafka_metadata_partition *p = &t->partitions[j];
                                if (NULL != p) {
                                    partitions.insert(p->id);
                                }
                            }
                        }
                    }
                }

                rd_kafka_metadata_destroy(metadata);
            }

            rd_kafka_topic_destroy(rd_kafka_topic);
        }
    }

    return !partitions.empty();
}

bool KtKafkaOffsetResetter::getTopicPartitionWaterMark(const std::string& topic,
            int32_t partition,
            PartitionWaterMark& water_mark) {
    bool rt = false;
    
    if (!topic.empty() &&
                initRdKafkaHandle()) {
        int retry = 0;
        rd_kafka_resp_err_t err;
RETRY:
        if (RD_KAFKA_RESP_ERR_NO_ERROR != (err = rd_kafka_query_watermark_offsets(rd_kafka_handle_,
                            topic.c_str(),
                            partition, &water_mark.lo, &water_mark.hi, 500))) {
            if (++retry < 4) {
                goto RETRY;
            }
       }

        rt = (RD_KAFKA_RESP_ERR_NO_ERROR == err);
    }

    return rt;
}

void KtKafkaOffsetResetter::resetByKF(std::set<int32_t> partitions,
            int reset_offset_strategy) {
    if (!topic_.empty() &&
                !group_.empty() &&
                initRdKafkaHandle()) {
        std::cout << "Start reset..." << std::endl;

        if (partitions.empty()) {
            std::set<int32_t> all_partitions;
            if (!getTopicPartitionList(topic_, all_partitions)) {
                std::cout << "Failed to getTopicPartitionList | "
                    << "topic:" << topic_ << std::endl
                    << std::endl;
                return;
            }
            partitions = all_partitions;
        }

        rd_kafka_topic_list_ = rd_kafka_topic_partition_list_new(1);
        if (NULL != rd_kafka_topic_list_) {
            for (std::set<int32_t>::iterator i = partitions.begin(), e = partitions.end();
                        i != e; ++i) {
                rd_kafka_topic_partition_t* res = rd_kafka_topic_partition_list_add(rd_kafka_topic_list_,
                            topic_.c_str(),
                            *i);
                if (NULL == res) {
                    std::cout << "Failed to rd_kafka_topic_partition_list_add"
                                << " | partiton: " << *i
                                << std::endl;
                    continue;
                }
            }

            if (!partitions.empty()) {
                for (std::set<int32_t>::iterator i = partitions.begin(), e = partitions.end();
                            i != e; ++i) {
                    PartitionWaterMark water_mark;
                    if (getTopicPartitionWaterMark(topic_, *i, water_mark)) {
                        int32_t partition = *i;

                        rd_kafka_resp_err_t err = rd_kafka_topic_partition_list_set_offset(rd_kafka_topic_list_,
                                    topic_.c_str(),
                                    partition,
                                    reset_offset_strategy == 0 ? water_mark.lo : water_mark.hi);
                        if (RD_KAFKA_RESP_ERR_NO_ERROR != err) {
                            std::cout << "Failed to rd_kafka_topic_partition_list_set_offset | partition: " << partition
                                << " | err msg:" << rd_kafka_err2str(err)
                                << std::endl;
                        } 

                    } else {
                        std::cout << "Failed to getTopicPartitionWaterMark | "
                            << "| partition: "<< *i
                            << std::endl;
                    }
                }

                rd_kafka_subscribe(rd_kafka_handle_, rd_kafka_topic_list_); 
                while (!rebalance_done_) {
                    rd_kafka_consumer_poll(rd_kafka_handle_, 100);
                }

                rd_kafka_resp_err_t err = rd_kafka_commit(rd_kafka_handle_, rd_kafka_topic_list_, 0);
                if (RD_KAFKA_RESP_ERR_NO_ERROR != err) {
                    std::cout << "Failed to rd_kafka_commit | err msg:" << rd_kafka_err2str(err)
                        << std::endl;
                } else {
                    rd_kafka_unsubscribe(rd_kafka_handle_);
                    std::cout << "Reset OK" << std::endl;
                }

            }
            rd_kafka_topic_partition_list_destroy(rd_kafka_topic_list_);
            rd_kafka_topic_list_ = NULL; 
        }
    }
}

void KtKafkaOffsetResetter::rdkafka_rebalance_cb(rd_kafka_t *rk,
            rd_kafka_resp_err_t err,
            rd_kafka_topic_partition_list_t *partitions,
            void *opaque) {
    KtKafkaOffsetResetter* self = static_cast<KtKafkaOffsetResetter*>(opaque);
    if (NULL != self) {
        ((KtKafkaOffsetResetter*)opaque)->rebalance_done_ = true;
    }

    switch (err) {
        case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
            {
                rd_kafka_assign(rk, partitions);
                //rd_kafka_assign(rk, NULL);
            }
            break;
        case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
            {
                rd_kafka_assign(rk, NULL);
            }
            break;
        default:
            rd_kafka_assign(rk, NULL);
            break;
    }
}

bool KtKafkaOffsetResetter::checkGroupActive(const std::string& topic,
            const std::string& group) {
    bool rt = false;
    if (!initRdKafkaHandle()) {
        return rt;
    }

    int retry = 0;

    const rd_kafka_group_list *grplistp;
RETRY:
    rd_kafka_resp_err_t err = rd_kafka_list_groups(rd_kafka_handle_,
                group.c_str(),
                &grplistp,
                500);
    if (++retry < 4) {
        goto RETRY;
    }

    if (RD_KAFKA_RESP_ERR_NO_ERROR == err) {
        //std::cout << "1: " << grplistp->group_cnt << std::endl;
        for (int i = 0; i< grplistp->group_cnt; ++i) {
            rd_kafka_group_info* info = (rd_kafka_group_info*)(grplistp->groups) + i;
            if (NULL != info) {
                if (NULL == info->group ||
                            NULL == info->state ||
                            strcmp(info->state, "Stable")) {
                    continue;
                }

                for (int j = 0; j < info->member_cnt; ++j) {
                    rd_kafka_group_member_info* member = (rd_kafka_group_member_info*)(info->members) + j;
                    if (NULL != member) {
                        unsigned char* metadata = (unsigned char*)member->member_metadata;
                        if (NULL != metadata) {
                            metadata = (unsigned char*)(metadata) + sizeof(int16_t);//jump version
                            if (NULL != metadata) {
                                int32_t topic_cnt = ntohl(*(int32_t*)(metadata));
                                metadata = (unsigned char*)(metadata) + sizeof(int32_t);//jump topic_cnt
                                for (int n = 0; n < topic_cnt; ++n) {
                                    int16_t topic_leng = ntohs(*(int16_t*)(metadata));
                                    metadata = (unsigned char*)(metadata) + sizeof(int16_t);//jump topic length
                                    std::string currentTopic(metadata, metadata + topic_leng);
                                    if (topic == currentTopic) {
                                        rt = true; 
                                        break;
                                    }
                                    metadata = (unsigned char*)(metadata) + topic_leng;//jump topic
                                }
                            }
                        } 
                    }
                } //for j

                if (rt) {
                    break;
                }
            }
        } //for i

        rd_kafka_group_list_destroy(grplistp);
    }

    return rt;
}

bool KtKafkaOffsetResetter::setRdKafkaConfig(rd_kafka_conf_t* rd_kafka_conf,
            const char* item,
            const char* value) {
   bool rt = false;

   if (NULL != rd_kafka_conf &&
               NULL != item &&
               0 != item[0] &&
               NULL != value &&
               0 != item[0]) {
       char err_str[512] = {0};

       if (RD_KAFKA_CONF_OK != rd_kafka_conf_set(rd_kafka_conf,
                       item,
                       value,
                       err_str,
                       sizeof(err_str))) {
           std::cout << __FUNCTION__ << " | Failed to rd_kafka_conf_set | item: " << item
                   << " | value: " << value
                   << " | error msg: " << err_str
                   << std::endl;
       } else {
           rt = true;
       }
   } else {
       std::cout << __FUNCTION__ << " | invailed parameter!"
           << " | item: " << item
           << " | value: " << value
           << std::endl;
   }

   return rt;
}

}//namespace kt 
