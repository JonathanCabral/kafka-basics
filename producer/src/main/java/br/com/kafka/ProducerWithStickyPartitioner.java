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

public class ProducerWithStickyPartitioner {

    private static final Logger log = LoggerFactory.getLogger(ProducerWithStickyPartitioner.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hi Im a Kafka producer");
        
        //Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create the Producer
        KafkaProducer<String, String>  producer = new KafkaProducer<String, String>(properties);

        //If you send lots of message in a small amoutn of time, kafka is smart enough to, batch this messages
        //in one batch to make more efficient, and send it to the same partition.
        //In this case, all the 10 messages are going to the same partition
        for (int i = 0; i < 10; i++) {
            //create the record
            ProducerRecord<String, String> record = new ProducerRecord<>("demo_topic", "Producing with a sticky partitoner " + i);

            //send data - asynchronous
            producer.send(record, (metadata, exception) -> {
                //This method is called everytime a message is sent OR an Exception is thrown
                if(exception == null) {
                    String message = new StringBuilder("Received metadata \n")
                            .append("Topic: ").append(metadata.topic()).append("\n")
                            .append("Partition: ").append(metadata.partition()).append("\n")
                            .append("Offset: ").append(metadata.offset()).append("\n")
                            .append("TimeStamp: ").append(metadata.timestamp()).toString();
                    log.info(message);
                } else log.error("Error while sending the message to a topic", exception);
            });
        }

        //Flush and close the Producer - asynchronous
        producer.flush();
        producer.close(); //producer.close also does the flush operation
    }
}
