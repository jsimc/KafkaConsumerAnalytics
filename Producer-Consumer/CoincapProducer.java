package app;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class CoincapProducer {
    private final static Logger log = LoggerFactory.getLogger(CoincapProducer.class.getSimpleName());

    /**
     * Obtaining different structured data.
     *
     * 1. assets for all monets (valute)
     * 2. bitcoin prices and dates.
     * 3. ethereum prices and dates.
     *
     * @param args args[0] is topic, args[1] is api url
     * @throws InterruptedException
     * @throws JsonProcessingException
     */
    public static void main(String[] args) throws InterruptedException, JsonProcessingException {
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.10.84:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = args[0]; // "topic-3";

        String coinCapApiUrl = args[1]; //"https://api.coincap.io/v2/assets/bitcoin/history?interval=d1";
        List<JsonNode> forSending = new ArrayList<>();
        // Fetch data from CoinCap API
        String coinCapData = fetchData(coinCapApiUrl);
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode json = objectMapper.readTree(coinCapData);
        json.fields().forEachRemaining(entity -> {
            // Entity is 100% data so:
            Iterator<JsonNode> elements = entity.getValue().elements();
            while(elements.hasNext()) {
                JsonNode row = elements.next();
                forSending.add(row);
            }
        });
        // Send the data to the Kafka topic
        for(int i = 0; i < forSending.size(); i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, String.valueOf(i), objectMapper.writeValueAsString(forSending.get(i)));
            producer.send(record);
        }

        // Close the Kafka producer
        TimeUnit.MINUTES.sleep(10);

        producer.close();

    }

    private static String fetchData(String apiUrl) {
        HttpClient httpClient = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(apiUrl))
                .build();

        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            return response.body();
        } catch (Exception e) {
            log.warn(e.getMessage());
            return null;
        }
    }
}
