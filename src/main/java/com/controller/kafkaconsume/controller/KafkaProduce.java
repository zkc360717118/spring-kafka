package com.controller.kafkaconsume.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.controller.kafkaconsume.config.KafkaSendResultHandler;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@RestController
public class KafkaProduce {
    @Autowired
    private AdminClient adminClient;

    @Autowired
    private KafkaTemplate defaultKafkaTemplate; //如果写成kafkaTemplate 就会调用类中的不带默认topic的KafkaTemplate

    @Autowired
    private KafkaSendResultHandler kafkaSendResultHandler;

    @Autowired
    private ReplyingKafkaTemplate replyingKafkaTemplate;
    /**
     * 手动创建topic
     */
    @RequestMapping("/createTopic")
    public void test(){
        System.out.println(" i am in");
        NewTopic topic = new NewTopic("initial2", 1, (short) 1);

        adminClient.createTopics(Arrays.asList(topic));
    }

    /**
     * 测试循环发送
     */
    @RequestMapping("send")
    public void send(){
        for (int i = 0; i < 12; i++) {
            defaultKafkaTemplate.send("batch", "test batch listener,dataNum-" + i);
        }
    }
    @RequestMapping("sendone")
    public void test2(){
        //使用配置好的默认template发送消息,默认topic是inital2
        defaultKafkaTemplate.sendDefault("I`m send msg to default topic");
        //发送带有时间戳的消息 ,指定topic
        defaultKafkaTemplate.send("first", 0, System.currentTimeMillis(), 0, "send message with timestamp");

        //使用ProducerRecord发送消息
        ProducerRecord record = new ProducerRecord("first", "use ProducerRecord to send message");
        defaultKafkaTemplate.send(record);

        //使用Message发送消息
        Map map = new HashMap();
        map.put(KafkaHeaders.TOPIC, "first");
        map.put(KafkaHeaders.PARTITION_ID, 0);
        map.put(KafkaHeaders.MESSAGE_KEY, 0);
        GenericMessage message = new GenericMessage("use Message to send message",new MessageHeaders(map));
        defaultKafkaTemplate.send(message);
    }



    /**
     * 带异步回调的消息发送，异步回调返回消息是否发送成功或者失败
     */
    @RequestMapping("sendtwo")
    public void test3(){
        defaultKafkaTemplate.setProducerListener(kafkaSendResultHandler);
        defaultKafkaTemplate.send("first", "test producer listen");
    }

    /**
     * 同步的消息发送，同步返回消息是否发送成功或者失败
     *  注意：get方法可以跟时间参数，表示必须在多少时间内发送完毕。如果值很小有时候会报错，但是消息最后还是发送成功了
     */
    @RequestMapping("sendthree")
    public void test4() throws ExecutionException, InterruptedException, TimeoutException {
        defaultKafkaTemplate.send("first", "test send message timeout").get();
        defaultKafkaTemplate.send("first", "test send message timeout2").get(1,TimeUnit.SECONDS);
    }

    /**
     * 事务方式一
     * 这里要使用KafkaConfiguration中的事务配置
     * 配置类中producerFactory 要配置事务
     *          transactionManager 要配置事务
     * @throws InterruptedException
     */
    @RequestMapping("/transone")
    @Transactional
    public void testTransactionalAnnotation() throws InterruptedException {
        defaultKafkaTemplate.send("first", "test transactional annotation--11");
        throw new RuntimeException("fail");
    }

    /**
     * 事务方式2
     * @throws InterruptedException
     */
    @RequestMapping("/transtwo")
    @Transactional
    public void testTransactionalAnnotation2() throws InterruptedException {
        defaultKafkaTemplate.executeInTransaction(operations -> {
            defaultKafkaTemplate.send("first", "test executeInTransaction--22");
//            throw new RuntimeException("fail");
            return true;
        });
    }

    /**
     * 我们需要创建ProducerRecord类，用来发送消息，并添加KafkaHeaders.REPLY_TOPIC到record的headers参数中，这个参数配置我们想要转发到哪个Topic中。
     * 使用replyingKafkaTemplate.sendAndReceive()方法发送消息，该方法返回一个Future类RequestReplyFuture，这里类里面包含了获取发送结果的Future类和获取返回结果的Future类。使用replyingKafkaTemplate发送及返回都是异步操作。
     * 调用RequestReplyFuture.getSendFutrue().get()方法可以获取到发送结果
     * 调用RequestReplyFuture.get()方法可以获取到响应结果
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws TimeoutException
     */
    @RequestMapping("/replytest")
    public void testReplyingKafkaTemplate() throws ExecutionException, InterruptedException, TimeoutException {
        //新建一条消息
        ProducerRecord<String, String> record = new ProducerRecord<>("first", "this is a message");
        //设置消息中的header,设置回复到哪个topic中
        record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, "second".getBytes()));
        //调用消息发送类发送消息
        RequestReplyFuture<String, String, String> replyFuture = replyingKafkaTemplate.sendAndReceive(record);
        //获取发送结果
        SendResult<String, String> sendResult = replyFuture.getSendFuture().get();
        System.out.println("Sent ok: " + sendResult.getRecordMetadata());
        //获取响应结果
        ConsumerRecord<String, String> consumerRecord = replyFuture.get();
        System.out.println("Return value: " + consumerRecord.value());
        Thread.sleep(20000);
    }


}
