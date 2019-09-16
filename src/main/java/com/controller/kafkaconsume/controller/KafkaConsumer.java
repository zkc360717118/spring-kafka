package com.controller.kafkaconsume.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
@EnableScheduling  //开启定时任务功能
public class KafkaConsumer {

    //为了定时任务 能够调用对应的消费方法
    @Autowired
    private KafkaListenerEndpointRegistry registry;

    /**
     * @KafkaListener属性解释
     * id：消费者的id，当GroupId没有被配置的时候，默认id为GroupId
     * containerFactory：上面提到了@KafkaListener区分单数据还是多数据消费只需要配置一下注解的containerFactory属性就可以了，这里面配置的是监听容器工厂，也就是ConcurrentKafkaListenerContainerFactory，配置BeanName
     * topics：需要监听的Topic，可监听多个
     * topicPartitions：可配置更加详细的监听信息，必须监听某个Topic中的指定分区，或者从offset为200的偏移量开始监听
     * errorHandler：监听异常处理器，配置BeanName
     * groupId：消费组ID
     * idIsGroup：id是否为GroupId
     * clientIdPrefix：消费者Id前缀
     * beanRef：真实监听容器的BeanName，需要在 BeanName前加 "__"
     */

    /**
     * 监听topic1主题,单条消费
     */
//    @KafkaListener(id = "consumer",topics = "${kafka.consumer.topic}")
//    public void listen0(ConsumerRecord<Integer, String> record) {
//        System.out.println(record);
//        System.out.println("消费"+record.value());
//        consumer(record);
//    }


    // 批量消费
    @KafkaListener(id = "batch",clientIdPrefix = "batch",topics = {"${topic.quick.batch}"},containerFactory = "batchContainerFactory")
    public void batchListener(List<String> data) {
        log.info("batch  receive : ");
        for (String s : data) {
            log.info(  s);
        }
    }


    /**
     *     消息转发方式1 sendto
     *      1,配置kafkaListenerContainerFactory中的 kafkaTemplate
     *          factory.setReplyTemplate(kafkaTemplate())
     *      2, 使用下面的注解，并写上需要转发的目标topic
     *      3,方法会把逻辑处理完成的返回值返回给目标队列
     *
     */
//    @KafkaListener(id="forward",topics = "first")
//    @SendTo("second")
//    public String forward(String data) {
//        log.info("fist  forward "+data+" to  second");
//        return "first send msg : " + data;
//    }


    /**
     * 定时启动消费，关闭自启动
     * @param data
     */
//    @KafkaListener(id = "durable", topics = "durabletopic",containerFactory = "delayContainerFactory")
//    public void durableListener(String data) {
//        //这里做数据持久化的操作
//        log.info("定时消费 receive : " + data);
//    }

    //定时器，每天17点开启监听
//    @Scheduled(cron = "0 18 17 * * ?")
//    public void startListener() {
//        log.info("开启监听");
//        //判断监听容器是否启动，未启动则将其启动
//        if (!registry.getListenerContainer("durable").isRunning()) {
//            registry.getListenerContainer("durable").start();
//        }
//        registry.getListenerContainer("durable").resume();
//    }

    //定时器，每天早上17点3分点关闭监听
//    @Scheduled(cron = "0 20 17 * * ?")
//    public void shutDownListener() {
//        log.info("关闭监听");
//        registry.getListenerContainer("durable").pause();
//    }

    /**
     * 含过滤的消费工厂，只消费消息结果是偶数的消息
     * filterContainerFactory
     * @param data
     */
//    @KafkaListener(id = "filterCons", topics = "first",containerFactory = "filterContainerFactory")
//    public void filterListener(String data) {
//        //这里做数据持久化的操作
//        log.error("过滤工厂 receive : " + data);
//    }

    /**
     * 测试异常处理
     * @param data
     */
    @KafkaListener(id = "err", topics = "first", errorHandler = "consumerAwareListenerErrorHandler")
    public void errorListener(String data) {
        log.info("topic.quick.error  receive : " + data);
        throw new RuntimeException("fail");
    }
}
