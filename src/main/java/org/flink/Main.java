package org.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Main {

    private static final String SAS_TOKEN_CONFIG = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"%s\";";
    private static final String SECURITY_PROTOCOL = "security.protocol";
    private static final String SASL_JAAS_CONFIG = "sasl.jaas.config";
    private static final String SASL_MECHANISM = "sasl.mechanism";

    public static void main(String[] args) throws Exception {

        // Kafka Source Properties

        String jaasStr = String.format(SAS_TOKEN_CONFIG, "Endpoint=sb://fdgate-eh-dev.servicebus.windows.net/;SharedAccessKeyName=fdgate-auth;SharedAccessKey=vJQBHUdSPsJ+6jZDSJPuqDl/vIMqAg+VrmhtJTMkJz0=;EntityPath=feed-gateway-ingress");
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "eis_pim_adp_aeh_feed_gateway_ingress_storm_grp_v2_dev_new__");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "eis_pim_adp_aeh_feed_gateway_ingress_storm_grp_v2_dev_new__");
        properties.put(SECURITY_PROTOCOL, "SASL_SSL");
        properties.put(SASL_MECHANISM, "PLAIN");
        properties.put(SASL_JAAS_CONFIG, jaasStr);


        // Kafka Source

        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        environment.getConfig().setParallelism(1);
        environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("fdgate-eh-dev.servicebus.windows.net:9093")
                .setProperties(properties)
                .setTopics("feed-gateway-ingress")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> dataStream =
                environment.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka source").uid("kafka-stream");

        dataStream.print();
        environment.execute("kafka read operation");
    }
}