package com.train.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {

    // 消费监听
    @KafkaListener(id="consumer2",topicPartitions = {
            @TopicPartition(topic = "testtopic",partitions = "0")},groupId = "felix-group",errorHandler = "consumerAwareErrorHandler")
//    @SendTo("testtopic")
    public String  onMessage2(ConsumerRecord<?, ?> record) throws Exception {
        // 消费的哪个topic、partition的消息,打印出消息内容
        System.out.println("简单消费2："+record.topic()+"-"+record.partition()+"-"+record.value());
//        throw new Exception("简单消费-模拟异常");
        return "11111";
    }


    @KafkaListener(id="consumer2-2",topicPartitions = {
            @TopicPartition(topic = "testtopic",partitions = "0")},groupId = "felix-group",errorHandler = "consumerAwareErrorHandler")
//    @SendTo("testtopic")
    public String  onMessage2_2(ConsumerRecord<?, ?> record) throws Exception {
        // 消费的哪个topic、partition的消息,打印出消息内容
        System.out.println("简单消费2-2："+record.topic()+"-"+record.partition()+"-"+record.value());
//        throw new Exception("简单消费-模拟异常");
        return "11111";
    }


    // 消费监听
//    @SendTo("testtopic")
    @KafkaListener(id="consumer3",topicPartitions = {
            @TopicPartition(topic = "testtopic",partitions = "1")},groupId = "felix-group",errorHandler = "consumerAwareErrorHandler")
    public String  onMessage3(ConsumerRecord<?, ?> record) throws Exception {
        // 消费的哪个topic、partition的消息,打印出消息内容
        System.out.println("简单消费3："+record.topic()+"-"+record.partition()+"-"+record.value());
//        throw new Exception("简单消费-模拟异常");
        return "11111";
    }

    @KafkaListener(id="consumer5",topicPartitions = {
            @TopicPartition(topic = "testtopic",partitions = "2")},errorHandler = "consumerAwareErrorHandler")
    public void onMessage5(ConsumerRecord<?, ?> record) throws Exception {
        // 消费的哪个topic、partition的消息,打印出消息内容
        System.out.println("简单消费5：" + record.topic() + "-" + record.partition() + "-" + record.value());
//        throw new Exception("简单消费-模拟异常");
    }

//    // 消费监听
//    @KafkaListener(topics = {"topic1"})
//    public void onMessage1(ConsumerRecord<?, ?> record){
//        // 消费的哪个topic、partition的消息,打印出消息内容
//        System.out.println("简单消费1："+record.topic()+"-"+record.partition()+"-"+record.value());
//    }
//
//    // 消费监听
//    @KafkaListener(topics = {"topic1"})
//    public void onMessage3(ConsumerRecord<?, ?> record){
//        // 消费的哪个topic、partition的消息,打印出消息内容
//        System.out.println("简单消费2："+record.topic()+"-"+record.partition()+"-"+record.value());
//    }



    /**
     * @Title 指定topic、partition、offset消费
     * @Description 同时监听topic1和topic2，监听topic1的0号分区、topic2的 "0号和1号" 分区，指向1号分区的offset初始值为8
     * @Author long.yuan
     * @Date 2020/3/22 13:38
     * @Param [record]
     * @return void
     **/

    /**
     * 属性解释：
     *
     * ① id：消费者ID；
     *
     * ② groupId：消费组ID；
     *
     * ③ topics：监听的topic，可监听多个；
     *
     * ④ topicPartitions：可配置更加详细的监听信息，可指定topic、parition、offset监听。
     * @param record
     * 上面onMessage2监听的含义：监听topic1的0号分区，同时监听topic2的0号分区和topic2的1号分区里面offset从8开始的消息。
     */
//    @KafkaListener(id = "consumer1",groupId = "felix-group",topicPartitions = {
//            @TopicPartition(topic = "testtopic",partitions = "3")})
////            @TopicPartition(topic = "topic2", partitions = "0", partitionOffsets = @PartitionOffset(partition = "1", initialOffset = "8"))
//
//    public void onMessage1(ConsumerRecord<?, ?> record) {
//        System.out.println("consumer1消费");
//        System.out.println("topic:"+record.topic()+"|partition:"+record.partition()+"|offset:"+record.offset()+"|value:"+record.value());
//    }
}
