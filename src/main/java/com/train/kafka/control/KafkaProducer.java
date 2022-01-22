package com.train.kafka.control;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;

@RestController
@Api(tags = "kafka")
@Slf4j
@Validated
public class KafkaProducer {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;
    // 发送消息
    @GetMapping("/kafka/normal")
    @ApiOperation(value = "发送消息", httpMethod = "GET")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "normalMessage", value = "", required = true, dataType = "String", paramType = "query"),
    })
    public void sendMessage1(HttpServletRequest request) {
        String normalMessage = request.getParameter("normalMessage");
        kafkaTemplate.send("testtopic", normalMessage);
    }

    @GetMapping("/kafka/callbackOne")
    @ApiOperation(value = "发送消息(带回调)", httpMethod = "GET")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "normalMessage", value = "", required = true, dataType = "String", paramType = "query"),
    })
    public void sendMessage2(HttpServletRequest request) {
        String callbackMessage = request.getParameter("normalMessage");
        kafkaTemplate.send("testtopic", callbackMessage).addCallback(success -> {
            // 消息发送到的topic
            String topic = success.getRecordMetadata().topic();
            // 消息发送到的分区
            int partition = success.getRecordMetadata().partition();
            // 消息在分区内的offset
            long offset = success.getRecordMetadata().offset();
            System.out.println("发送消息成功:" + topic + "-" + partition + "-" + offset);
        }, failure -> {
            System.out.println("发送消息失败:" + failure.getMessage());
        });
    }


    @GetMapping("/kafka/transaction")
    @ApiOperation(value = "发送消息(带回调)", httpMethod = "GET")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "normalMessage", value = "", required = true, dataType = "String", paramType = "query"),
    })
    public void sendMessage7(HttpServletRequest request){
        // 声明事务：后面报错消息不会发出去
        kafkaTemplate.executeInTransaction(kafkaOperations -> {
            return kafkaOperations.send("testtopic","test executeInTransaction");
        });
        // 不声明事务：后面报错但前面消息已经发送成功了
//        kafkaTemplate.send("topic1","test executeInTransaction");
//        throw new RuntimeException("fail");

//        kafkaTemplate.executeInTransaction(operations -> {
//            operations.send("topic1","test executeInTransaction");
//            throw new RuntimeException("fail");
//        });
        // 不声明事务：后面报错但前面消息已经发送成功了
//        kafkaTemplate.send("topic1","test executeInTransaction");
//        throw new RuntimeException("fail");
    }


}
