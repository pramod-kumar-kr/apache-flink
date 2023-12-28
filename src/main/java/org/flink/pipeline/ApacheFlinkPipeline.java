package org.flink.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPipelineException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Headers;
import org.flink.Main;
import org.flink.model.KafkaInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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


        LocalDateTime localDateTime = LocalDateTime.now();
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
        KafkaSource<KafkaInput> kafkaSource = KafkaSource.<KafkaInput>builder()
                .setBootstrapServers("fdgate-eh-dev.servicebus.windows.net:9093")
                .setProperties(properties)
                .setTopics("feed-gateway-ingress")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new KafkaRecordDeserializationSchema<KafkaInput>(){

                    @Override
                    public TypeInformation<KafkaInput> getProducedType() {
                        return TypeInformation.of(KafkaInput.class);
                    }

                    @Override
                    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<KafkaInput> out) throws IOException {
                        long offset = consumerRecord.offset();
                        int partition = consumerRecord.partition();
                        Headers headers = consumerRecord.headers();
                        String key = new String(consumerRecord.key());
                        String value = new String(consumerRecord.value());
                        out.collect(new KafkaInput(headers, key, value, partition, offset));
                    }
                })
                .build();

        DataStream<KafkaInput> dataStream =
                environment.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka source").uid("kafka-stream");


        //JSON parsing

        AtomicReference<Long> count = new AtomicReference<>(0L);

        DataStream<KafkaInput> processedStreamKafkaInput = dataStream.process(new ProcessFunction<KafkaInput, KafkaInput>() {
            @Override
            public void processElement(KafkaInput value, ProcessFunction<KafkaInput, KafkaInput>.Context ctx, Collector<KafkaInput> collector) throws Exception {
                collector.collect(value);
            }
        }).name("Process-1");

        DataStream<KafkaInput> filteredDataStream = processedStreamKafkaInput.filter(new FilterFunction<KafkaInput>() {
            @Override
            public boolean filter(KafkaInput value) throws Exception {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode jsonNode = mapper.readTree(value.getValue());
                String traceIdValue = jsonNode.get("header").get("transactionType").asText();
                return traceIdValue.equals("ADD");
            }
        }).name("Filter");

//        filteredDataStream.print();

        DataStream<String> valueProcess = filteredDataStream.process(new ProcessFunction<KafkaInput, String>() {
            @Override
            public void processElement(KafkaInput value, ProcessFunction<KafkaInput, String>.Context ctx, Collector<String> out) throws Exception {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode jsonNode = mapper.readTree(value.getValue());
                String traceIdValue = jsonNode.get("header").get("TraceId").asText();
                out.collect(traceIdValue);
            }
        });

        DataStream<String> appendedProcess = valueProcess.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value + localDateTime;
            }
        });

        appendedProcess.print();
        try {
            environment.execute("kafka read operation");
        } catch (Exception e) {
            throw new ChannelPipelineException(e);
        }
    }
}
