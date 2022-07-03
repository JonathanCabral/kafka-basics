package br.com.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerCooperativeRebalanceMode {

    private static final Logger log = LoggerFactory.getLogger(ConsumerCooperativeRebalanceMode.class.getSimpleName());

    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-first-cg");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().
                addShutdownHook(new Thread() {
                    @Override
                    public void run() {
                        consumer.wakeup();

                        try {
                            mainThread.join();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                });


        try {
            consumer.subscribe(Arrays.asList("demo_topic"));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(record -> {
                    log.info("Message -> " + record.value());
                });
            }
        } catch (WakeupException e) {
            log.warn("wake up ex, expected");
        } catch (Exception e) {
            log.error("error");
        } finally {
            consumer.close();
        }
    }
}
