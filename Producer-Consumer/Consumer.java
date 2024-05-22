package app;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    private final static Logger log = LoggerFactory.getLogger(Producer.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Kafka Consumer");

        String filePath = "data.csv";

        String groupId = "kafka-februar";
        String topic = "topic-1";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.10.108:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Arrays.asList(topic));

        while(true) {
            log.info("Polling...");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord<String, String> record : records) {
                try {
                    // Create a FileWriter and BufferedWriter
                    FileWriter fileWriter = new FileWriter(filePath, true);
                    BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
                    // Write data rows
                    bufferedWriter.write(record.value());
                    bufferedWriter.newLine();
                    // Close the BufferedWriter
                    bufferedWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                log.info("Key: " + record.key() + ", value: " + record.value() + "\n" +
                         "Partition: " + record.partition() + ", Offset: " + record.offset() + "\n");
            }
        }
    }

//    private static void writeRow(BufferedWriter writer, String[] data) throws IOException {
//        // Write each element in the data array, separated by commas
//        for (int i = 0; i < data.length; i++) {
//            writer.write(data[i]);
//            // Add a comma after each element, except for the last one
//            if (i < data.length - 1) {
//                writer.write(",");
//            }
//        }
//        // Add a new line after each row
//        writer.newLine();
//    }
}
