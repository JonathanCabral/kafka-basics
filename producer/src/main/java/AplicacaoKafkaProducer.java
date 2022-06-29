import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class AplicacaoKafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(AplicacaoKafkaProducer.class.getSimpleName());

    public static void main(String[] args) {

        //Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create the Producer
        KafkaProducer<String, String>  producer = new KafkaProducer<String, String>(properties);

        //create the record
        ProducerRecord<String, String> record = new ProducerRecord<>("demo_topic", "Hello Kafka Java");

        //send data - asynchronous
        producer.send(record);

        //Flush and close the Producer - asynchronous
        producer.flush();
        producer.close(); //producer.close also does the flush operation


    }
}
