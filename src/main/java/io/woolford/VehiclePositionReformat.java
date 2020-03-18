package io.woolford;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class VehiclePositionReformat {

    private static Logger logger = LoggerFactory.getLogger(VehiclePositionReformat.class);

    public static void main(String[] args) {

        // set props for Kafka Steams app (see KafkaConstants)
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, KafkaConstants.APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.KAFKA_BROKERS);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> vehiclePositionStream = builder.stream("vehicle-positions");

        vehiclePositionStream.mapValues(value -> {
            value = VehiclePositionReformat.transformJson(value);
            return value;
        }).to("vehicle-positions-enriched");

        // run it
        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    private static String transformJson(String json){

        ObjectMapper mapper = new ObjectMapper();

        Map jsonMap = new HashMap<>();

        try {
            jsonMap = mapper.readValue(json, Map.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        Map result = (Map) jsonMap.get("VP");

        if (result.get("lat") != null && result.get("long") != null){

            //Latitudes range from 0 to 90. Longitudes range from 0 to 180
            Double latitude = (Double) result.get("lat");
            Double longitude = (Double) result.get("long");

            if (0 <= latitude && latitude <= 90 && 0 <= longitude && longitude <= 180){

                // create Elastic-friendly geojson
                Map<String, Double> location = new HashMap<>();
                location.put("lat", latitude);
                location.put("lon", longitude);

                result.put("location", location);

            }
        }

        result.remove("lat");
        result.remove("long");

        String resultJson = null;
        try {
            resultJson = mapper.writeValueAsString(result);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        return resultJson;
    }

}
