package com.sandeep.notification.service.domain.message;


import java.util.Iterator;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;

public class KafkaMessageConsumer implements SmartLifecycle {


  public static final Logger LOGGER = LoggerFactory.getLogger(KafkaMessageConsumer.class);

  private List<String> topicNames;
  private boolean isRunning = false;
  private boolean isStartUp = true;
  private ThreadPoolTaskExecutor executor;

  @Autowired
  private KafkaConsumer<String, String> consumer;

  public void startConsumerService(List<String> consumerList) {
    try {
      Assert.notNull(consumerList, "Topic List Should Not Be Empty");
      Assert.notEmpty(consumerList, "topics names should not be empty");
      consumer.subscribe(consumerList);
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
        Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
        while (iterator.hasNext()) {
          ConsumerRecord<String, String> consumerRecord = iterator.next();
          System.out.println(consumerRecord.value());
        }
      }
    } catch (WakeupException ex) {
      consumer.close();
    }
  }

  public void stopConsumer() {
    if (isRunning) {
      consumer.wakeup();
      executor.setWaitForTasksToCompleteOnShutdown(true);
      executor.shutdown();
      this.isRunning = false;
    }
  }
  
  
  public void connectToReceive(List<String> topicNames){
    if(!isRunning){
      executor = new ThreadPoolTaskExecutor();
      executor.afterPropertiesSet();
      executor.submit(() -> startConsumerService(topicNames));
    }
  }

  @Override
  public boolean isRunning() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void start() {
    // TODO Auto-generated method stub

  }

  @Override
  public void stop() {
    // TODO Auto-generated method stub

  }

  @Override
  public int getPhase() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean isAutoStartup() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void stop(Runnable arg0) {
    // TODO Auto-generated method stub

  }

}
