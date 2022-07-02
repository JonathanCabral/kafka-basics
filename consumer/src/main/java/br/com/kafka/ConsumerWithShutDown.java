package br.com.kafka;


import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;


//Shutdown consumer gracefully.
//When a consumer is closed right, kafka is able to close metrics, unregistry the cg and commit the offsets.
public class ConsumerWithShutDown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerWithShutDown.class.getSimpleName());

    public static void main(String[] args) {

        //Prepare properties for consumer connection
        Properties props = new Properties();
        props.setProperty(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-third-cg");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create new kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
// -------------------- Adding a shutdown hook
        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("Detected a shutdown, let's exit by calling a consumer.wakeup");
                // When consumer.wakepu is called, an exception is thrown, WakeupException.
                //This is used normal to abort a long poll
                consumer.wakeup();

                //Join the main thread to allow the execution of the code in the main thread.
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
//-------------------------

        try {
            //subscribe to one or many topics
            consumer.subscribe(Arrays.asList("demo_topic"));
            //consume the messages
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                records.forEach(record -> {
                    log.info("Message " + record.value());
                    log.info("Key " + record.key());
                    log.info("partition " + record.partition());
                });
            }
        } catch (WakeupException we) {
            // We ignore at this as this is an expected exception when closing a consumer
            log.info("Expected wakeup exception");
        } catch (Exception e) {
            log.error("Unexpected exception");
        } finally {
            //This also commit the offsets if it needed
            consumer.close();
            log.info("The consumer is now gracefully closed");
        }
    }
}
