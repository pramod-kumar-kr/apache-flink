package org.flink.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.runtime.rpc.Local;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Headers;
import org.flink.Main;
import org.flink.model.KafkaInput;
import org.flink.model.KafkaRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class ApacheFlinkPipeline {

    public static class MockApiCallFunction extends RichMapFunction<KafkaInput, String> {

        @Override
        public String map(KafkaInput input) throws Exception {
            // Perform your mock API call logic here
            String mockApiResponse = mockApiCall(input);

            // You can log or perform any other processing based on the mock API response

            return "Processed Record: " + input + ", Mock API Response: " + mockApiResponse;
        }

        private String mockApiCall(KafkaInput input) {
            // Implement your mock API call logic here
            // For example, return a predefined response or generate a response based on the input
            return "Mock API Response for: " + input;
        }
    }

    public static class SimplePrintSink implements SinkFunction<String> {
        @Override
        public void invoke(String value, SinkFunction.Context context) {
            // Replace this with your actual sink logic (e.g., database insertion)
            System.out.println("Received: " + value);
        }
    }
    private static final String SAS_TOKEN_CONFIG = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"%s\";";
    private static final String SECURITY_PROTOCOL = "security.protocol";
    private static final String SASL_JAAS_CONFIG = "sasl.jaas.config";
    private static final String SASL_MECHANISM = "sasl.mechanism";

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {

        // Kafka Source Properties

//        String jaasStr = String.format(SAS_TOKEN_CONFIG, "Endpoint=sb://fdgate-eh-dev.servicebus.windows.net/;SharedAccessKeyName=fdgate-auth;SharedAccessKey=vJQBHUdSPsJ+6jZDSJPuqDl/vIMqAg+VrmhtJTMkJz0=;EntityPath=feed-gateway-ingress");
        Properties properties = new Properties();
//        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "eis_pim_adp_aeh_feed_gateway_ingress_storm_grp_v2_dev_new__");
//        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "eis_pim_adp_aeh_feed_gateway_ingress_storm_grp_v2_dev_new__");
//        properties.put(SECURITY_PROTOCOL, "SASL_SSL");
//        properties.put(SASL_MECHANISM, "PLAIN");
//        properties.put(SASL_JAAS_CONFIG, jaasStr);


        // Kafka Source

        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        environment.getConfig().setParallelism(4);
/*
        If checkpointing is not enabled, the “no restart” strategy is used.
        If checkpointing is activated and the restart strategy has not been configured,
        the fixed-delay strategy is used with Integer.MAX_VALUE restart attempts.
*/
        environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));
        KafkaSource<KafkaInput> kafkaSource = KafkaSource.<KafkaInput>builder()
                .setBootstrapServers("my-new-server-kafka.default.svc.cluster.local:9092")
                .setProperties(properties)
                .setTopics("test-topic")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new KafkaRecordDeserializationSchema<KafkaInput>() {


                    @Override
                    public TypeInformation<KafkaInput> getProducedType() {
                        return TypeInformation.of(KafkaInput.class);
                    }

                    @Override
                    public void open(DeserializationSchema.InitializationContext context) throws Exception {
                        KafkaRecordDeserializationSchema.super.open(context);
                    }

                    @Override
                    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<KafkaInput> collector) throws IOException {
                        long offset = consumerRecord.offset();
                        int partition = consumerRecord.partition();
                        Headers headers = consumerRecord.headers();
                        String value = new String(consumerRecord.value());
                        collector.collect(new KafkaInput(headers, null, value, partition, offset));
                    }
                })
                .build();

        DataStream<KafkaInput> dataStream =
                environment.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka source")
                        .uid("kafka-stream")
                        .name("DataSource");



       DataStream<String> apiCallDataStream = dataStream.map(new MockApiCallFunction())
               .name("API Call");

       DataStreamSink<String> sinkDataStream = apiCallDataStream.addSink(new SimplePrintSink())
               .name("Sink");

        apiCallDataStream.print();
        //JSON parsing

        AtomicReference<Long> count = new AtomicReference<>(0L);

        environment.executeAsync("Operation on 2.3 million data");
    }
}
