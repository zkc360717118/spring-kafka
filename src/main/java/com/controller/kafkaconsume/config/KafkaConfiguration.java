package com.controller.kafkaconsume.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ConsumerAwareListenerErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfiguration {
    @Value("${kafka.consumer.servers}")
    private String servers;

    @Value("${kafka.consumer.group.id}")
    private String cousumerGroup;
    @Value("${kafka.consumer.enable.auto.commit}")
    private String autoCommit;
    @Value("${kafka.consumer.session.timeout}")
    private int consumerSessionTimeout;
    @Value("${kafka.consumer.auto.commit.interval}")
    private int cousumerCommitInterval;
    @Value("${kafka.consumer.auto.offset.reset}")
    private String autoOffsetReset;
    @Value("${kafka.consumer.max-poll-records}")
    private int maxPollRecords;

    //app启动则初始化topic,设置分区数量和副本数
//    @Bean
//    public NewTopic initialTopic() {
//        return new NewTopic("initial3", 3, (short) 1);
//    }

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> props = new HashMap<>();
        //配置Kafka实例的连接地址
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        KafkaAdmin admin = new KafkaAdmin(props);
        return admin;
    }

    @Bean
    public AdminClient adminClient() {
        return AdminClient.create(kafkaAdmin().getConfig());
    }


    //批量消费工厂
    @Bean("batchContainerFactory")
    public ConcurrentKafkaListenerContainerFactory listenerContainer(){
        ConcurrentKafkaListenerContainerFactory<Object, Object> container = new ConcurrentKafkaListenerContainerFactory<>();
        container.setConsumerFactory(new DefaultKafkaConsumerFactory<>(consumerProps()));
        //设置并发量，小于或等于Topic的分区数
        container.setConcurrency(5);
        //设置批量监听
        container.setBatchListener(true);
        return container;
    }

    //单条消费配置工厂
    // ConcurrentKafkaListenerContainerFactory为创建Kafka监听器的工程类，这里只配置了消费者
    @Bean
    public ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<Integer, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setReplyTemplate(kafkaTemplate());//设置消息转发的kafka发送模板
        return factory;
    }

    //配置消费工厂，关闭定时启动
    @Bean
    public ConcurrentKafkaListenerContainerFactory delayContainerFactory(){
        ConcurrentKafkaListenerContainerFactory container = new ConcurrentKafkaListenerContainerFactory();
        container.setConsumerFactory(consumerFactory());
        //禁止自动启动
        container.setAutoStartup(false);
        return container;
    }

    //配置消费工厂，设置消息过滤器
    @Bean
    public ConcurrentKafkaListenerContainerFactory filterContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<Integer, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        //配合RecordFilterStrategy使用，被过滤的信息将被丢弃
        factory.setAckDiscarded(true);
        factory.setRecordFilterStrategy(new RecordFilterStrategy<Integer, String>() {
            @Override
            public boolean filter(ConsumerRecord<Integer, String> consumerRecord) {
                long data = Long.parseLong((String) consumerRecord.value());
                System.out.println("过滤工厂 ："+data);
                if(data%2 == 0){
                    return false;
                }
                return true;
            }
        });
        return factory;
    }

    //根据consumerProps填写的参数创建消费者工厂
    @Bean
    public ConsumerFactory<Integer, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerProps());
    }

    //配置消息转发
    @Bean
    public KafkaMessageListenerContainer<String, String> replyContainer(@Autowired ConsumerFactory consumerFactory) {
        ContainerProperties containerProperties = new ContainerProperties("second");//消息转发的对应topic
        return new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
    }

    //配置消息转发
    @Bean
    public ReplyingKafkaTemplate<String, String, String> replyingKafkaTemplate(@Autowired ProducerFactory producerFactory, KafkaMessageListenerContainer replyContainer) {
        ReplyingKafkaTemplate template = new ReplyingKafkaTemplate<>(producerFactory, replyContainer);
        template.setReplyTimeout(10000);
        return template;
    }

    //根据senderProps填写的参数创建生产者工厂
    @Bean
    public ProducerFactory<Integer, String> producerFactory() {
        DefaultKafkaProducerFactory<Integer, String> factory = new DefaultKafkaProducerFactory<>(senderProps());
//        factory.transactionCapable();//开启kafka事务 和发送客户端的@transactional配合使用，否则会报错
//        factory.setTransactionIdPrefix("trankevin-");//TransactionIdPrefix是用来生成Transactional.id的前缀
        return factory;
    }


    //kafka事务
//    @Bean
//    public KafkaTransactionManager transactionManager(ProducerFactory producerFactory) {
//        return new KafkaTransactionManager(producerFactory);
//    }

    //kafkaTemplate实现了Kafka发送接收等功能
    @Bean
//    @Primary //表示如果有多个同类型的Bean时，优先使用该Bean
    public KafkaTemplate<Integer, String> kafkaTemplate() {
        KafkaTemplate template = new KafkaTemplate<Integer, String>(producerFactory());
        return template;
    }
    //defaultKafkaTemplate实现了Kafka发送接收等功能 设置了默认的topic
    @Bean("defaultKafkaTemplate")
    public KafkaTemplate<Integer, String> defaultKafkaTemplate() {
        KafkaTemplate template = new KafkaTemplate<Integer, String>(producerFactory());
        template.setDefaultTopic("initial2"); //设置默认topic
        return template;
    }

    //单消息消费的异常处理器
    @Bean
    public ConsumerAwareListenerErrorHandler consumerAwareListenerErrorHandler() {
        return new ConsumerAwareListenerErrorHandler() {
            @Override
            public Object handleError(Message<?> message, ListenerExecutionFailedException exception, Consumer<?, ?> consumer) {
                System.out.println("consumerAwareErrorHandler receive : "+message.getPayload().toString());
                return null;
            }
        };
    }

    //批量消费异常处理
    @Bean
    public ConsumerAwareListenerErrorHandler consumerAwareErrorHandler() {
        return new ConsumerAwareListenerErrorHandler() {

            @Override
            public Object handleError(Message<?> message, ListenerExecutionFailedException e, Consumer<?, ?> consumer) {
                System.out.println("consumerAwareErrorHandler receive : "+message.getPayload().toString());
                MessageHeaders headers = message.getHeaders();
                //批量异常有问题，回头整理
//                List list = headers.get(KafkaHeaders.RECEIVED_TOPIC, List.class);
//                List partition = headers.get(KafkaHeaders.RECEIVED_PARTITION_ID, List.class);
//                List offset = headers.get(KafkaHeaders.OFFSET, List.class);
//                Map<TopicPartition, Long> offsetsToReset = new HashMap<>();

                return null;
            }
        };
    }


    //消费者配置参数
    private Map<String, Object> consumerProps() {
        Map<String, Object> props = new HashMap<>();
        //连接地址
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        //GroupID
        props.put(ConsumerConfig.GROUP_ID_CONFIG, cousumerGroup);
        //是否自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit);
        //自动提交的频率
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, cousumerCommitInterval);
        //Session超时设置
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        //键的反序列化方式
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        //值的反序列化方式
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        //批量消费参数配置
        //一次性拉取消息的数量5
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"5");
        return props;
    }

    //生产者配置
    private Map<String, Object> senderProps() {
        Map<String, Object> props = new HashMap<>();
        //连接地址
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        //重试，0为不启用重试机制
        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        //控制批处理大小，单位为字节
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        //批量发送，延迟为1毫秒，启用该功能能有效减少生产者发送消息次数，从而提高并发量
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        //生产者可以使用的总内存字节来缓冲等待发送到服务器的记录
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 1024000);
        //键的序列化方式
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        //值的序列化方式
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

}
