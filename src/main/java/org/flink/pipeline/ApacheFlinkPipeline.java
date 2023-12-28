package org.flink.pipeline;

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
import org.flink.Main;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class ApacheFlinkPipeline {
    private static final String SAS_TOKEN_CONFIG = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"%s\";";
    private static final String SECURITY_PROTOCOL = "security.protocol";
    private static final String SASL_JAAS_CONFIG = "sasl.jaas.config";
    private static final String SASL_MECHANISM = "sasl.mechanism";

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

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
/*
        If checkpointing is not enabled, the “no restart” strategy is used.
        If checkpointing is activated and the restart strategy has not been configured,
        the fixed-delay strategy is used with Integer.MAX_VALUE restart attempts.
*/
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


        //JSON parsing

        AtomicReference<Long> count = new AtomicReference<>(0L);


        DataStream<String> processedStream = dataStream.map((MapFunction<String, String>) jsonMessage -> {
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode jsonNode = mapper.readTree(jsonMessage);
                    String traceIdValue = jsonNode.get("header").get("traceId").asText();
                    JsonNode products = jsonNode.get("products").get(0).get("productId").get("gtin");
                    return " Timestamp : " + LocalDateTime.now() +  "  |  TraceId: " + traceIdValue + "  | products: " + products + "  | Count :" + count.getAndSet(count.get() + 1);
                })
                .name("JSON Processing Map")
                .uid("json-processing-map")
                .setParallelism(16)
                .setMaxParallelism(24);

        DataStream<String> map2Stream = processedStream.map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        return value;
                    }
                }).setParallelism(16)
                .setMaxParallelism(24);

        DataStream<String> mapStream = map2Stream.map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        return value + " | threadId : " + Thread.currentThread().getId();
                    }
                }).setParallelism(16)
                .setMaxParallelism(24);

        mapStream.print();
        environment.execute("kafka read operation");
    }
}
