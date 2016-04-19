package com.storm.kafka;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * Created by zhangxiaolei05 on 2016/4/14.
 */
public class KafkaAppender extends AppenderBase<ILoggingEvent> {
    private String topic;
    private String zookeeperHost;
    private Producer<String,String> producer;
    @Override
    protected void append(ILoggingEvent iLoggingEvent) {
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(this.topic,iLoggingEvent.getMessage());
        this.producer.send(data);
    }

    @Override
    public void start() {
        super.start();
        Properties props = new Properties();
        props.put("metadata.broker.list", this.zookeeperHost);
        props.put("serializer.class","kafka.serializer.StringEncoder");
        props.put("partitioner.class","kafka.producer.DefaultPartitioner");
        ProducerConfig config = new ProducerConfig(props);
        this.producer = new Producer<String, String>(config);
    }

    @Override
    public void stop() {
        super.stop();
        this.producer.close();
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getZookeeperHost() {
        return zookeeperHost;
    }

    public void setZookeeperHost(String zookeeperHost) {
        this.zookeeperHost = zookeeperHost;
    }

    public Producer<String, String> getProducer() {
        return producer;
    }

    public void setProducer(Producer<String, String> producer) {
        this.producer = producer;
    }
}
