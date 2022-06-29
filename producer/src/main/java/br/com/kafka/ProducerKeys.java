package br.com.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class ProducerKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hi Im a Kafka producer");
        
        //Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create the Producer
        KafkaProducer<String, String>  producer = new KafkaProducer<String, String>(properties);

        //Messages with the same key goes to the same partition
        for (int i = 0; i < 10; i++) {

            String topic = "demo_topic";
            String key = "id_"+i;
            String value = "Producing messages with ID " + i;

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            producer.send(record, (metadata, exception) -> {
                //This method is called everytime a message is sent OR an Exception is thrown
                if(exception == null) {
                    String message = new StringBuilder("Received metadata \n")
                            .append("Topic: ").append(metadata.topic()).append("\n")
                            .append("Key: ").append(record.key()).append("\n")
                            .append("Partition: ").append(metadata.partition()).append("\n").toString();
                    log.info(message);
                } else log.error("Error while sending the message to a topic", exception);
            });
        }

        producer.close(); //producer.close also does the flush operation
    }
}
