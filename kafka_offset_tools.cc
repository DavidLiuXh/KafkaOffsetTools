#include <string>
#include <iostream>
#include <sstream>

#include <boost/program_options.hpp>  

#include "kafka_offset_resetter.h"
//--------------------------------------------
namespace po = boost::program_options;
//--------------------------------------------
void split(std::string str,
            const std::string& pattern,
            std::set<int32_t >& result) {
    std::string::size_type pos;
    str += pattern;
    int size = str.size();

    for(int i = 0; i < size; ++i) {
        pos = str.find(pattern, i);
        if (pos < size) {
            std::string s = str.substr(i, pos - i);
            result.insert(atoll(s.c_str()));
            i = pos + pattern.size() - 1;
        }
    }
}

int main(int argc, char * argv[]) {
    std::string broker_list;
    std::string topic;
    std::string group;
    std::string partition_list;
    long long reset_offset_pos;

    po::options_description desc("Usage");  
    desc.add_options()  
        ("broker_list", po::value<std::string>(&broker_list)->default_value(""), "kafka broker list") 
        ("topic", po::value<std::string>(&topic)->default_value(""), "kafka topic name") 
        ("group", po::value<std::string>(&group)->default_value(""), "consumer group name") 
        ("partition_list", po::value<std::string>(&partition_list)->default_value(""), "reset partiton list\n\"\":all parition(default value)\n Or 1,2,3...") 
        ("reset_pos", po::value<long long>(&reset_offset_pos)->default_value(0), "reset paritions to position:\n0:earliest\n1:latest"); 

    po::variables_map vm;  
    try {
        po::store(po::parse_command_line(argc, argv, desc), vm);  
    } catch (...) {
        std::cout << desc << std::endl;
        return 1;
    }
    po::notify(vm);

    if (broker_list.empty() ||
                topic.empty() ||
                group.empty()) {
        std::cout << desc << std::endl;
        return 1;
    }

    kt::KtKafkaOffsetResetter resetter(broker_list, topic, group);
    if (resetter.init()) {
        std::set<int32_t> partitions;
        if (!partition_list.empty()) {
            split(partition_list, ",", partitions);
        }

        resetter.resetByKF(partitions, reset_offset_pos);

        resetter.uninit();
    } else {
        std::cout << "Failed to init" << std::endl;
    }
    
    return 0;
}
