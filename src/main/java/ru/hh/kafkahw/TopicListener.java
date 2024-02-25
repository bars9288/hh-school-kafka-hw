package ru.hh.kafkahw;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.internal.Service;
import ru.hh.kafkahw.KafkaProducer;

@Component
public class TopicListener {
  private final static Logger LOGGER = LoggerFactory.getLogger(TopicListener.class);
  private final Service service;

  @Autowired
  public TopicListener(Service service) {
    this.service = service;
  }

  @KafkaListener(topics = "topic1", groupId = "group1")
  /*
 Метод  отправляет каждое сообщение хотя бы один раз, при этом счетчик коммит-оффсет выполняет счет непрерывно;
 Выпадающие исключения RuntimeException() в Service-handle будут препятствовать записи сообщений в топик
 при этом операция инкремента будет обязательно выполнена
 очень хороший мануал помог (сслыка с таймингом) https://youtu.be/CNlsz6JKaI8?t=1193
   */
  public void atMostOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    ack.acknowledge();
    LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
    try {
      service.handle("topic1", consumerRecord.value());
    } catch (Exception ignore) { }
  }

  @KafkaListener(topics = "topic2", groupId = "group2")
    /*
 Метод  отправляет хотя бы один раз каждое сообщение при этом счетчик коммит-оффсет выполняет счет непрерывно;
 Выпадающие исключения RuntimeException() в Service-handle будут обязывать отправить текущее сообщение и только
 после успешной его отправки операция инкремента будет обязательно выполнена
мануал помог (сслыка с таймингом) https://youtu.be/CNlsz6JKaI8?t=1023
   */
  public void atLeastOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
    try {
    service.handle("topic2", consumerRecord.value());
    } finally {
      /* Если верить мануалам то acknowledgment позволяет гарантировать, что сообщение было успешно доставлено до того,
       как продюсер считает его отправленным. - поэтому тут блок finally
       */
    ack.acknowledge();
    }
  }

  @KafkaListener(topics = "topic3", groupId = "group3")
  // https://youtu.be/CNlsz6JKaI8?t=1897

  public void exactlyOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    ack.acknowledge();
    LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
    try {
      service.handle("topic3", consumerRecord.value());
    } finally {
      ack.acknowledge();
    }
  }

}
