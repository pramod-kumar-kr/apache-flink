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
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.rpc.Local;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
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
import java.math.BigInteger;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class ApacheFlinkPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(ApacheFlinkPipeline.class);

    private static final LocalDateTime startTime = LocalDateTime.now();

    public static class MockApiCallFunction extends RichMapFunction<KafkaInput, String> {
        private transient Counter counter;

        BigInteger myCounter = BigInteger.ZERO;
        private transient int valueToExpose = 0;


        @Override
        public void open(Configuration config) {
            this.counter = getRuntimeContext()
                    .getMetricGroup()
                    .counter("myCounter");

            getRuntimeContext()
                    .getMetricGroup()
                    .gauge("MyGauge", new Gauge<Integer>() {
                        @Override
                        public Integer getValue() {
                            return valueToExpose;
                        }
                    });
        }


        @Override
        public String map(KafkaInput input) throws Exception {
            this.counter.inc();
            valueToExpose++;
            // Perform your mock API call logic here
            String mockApiResponse = mockApiCall(input);
//            LOG.info(String.valueOf(input));
            myCounter = myCounter.add(BigInteger.ONE);
            Duration duration = Duration.between(LocalDateTime.now(), startTime);

            // Access the components of the duration
            long days = duration.toDays();
            long hours = duration.toHours() % 24;
            long minutes = duration.toMinutes() % 60;
            long seconds = duration.getSeconds() % 60;

            // Display the difference
            // You can log or perform any other processing based on the mock API response

            return "Count : " + myCounter + " StartTime : " + startTime + ", EndTime: " + LocalDateTime.now() +
                    " TimeTaken :" + hours + " hours, " + minutes + " minutes, " + seconds + " seconds.";
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
//            System.out.println("Received: " + value);
        }
    }
    private static final String SAS_TOKEN_CONFIG = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"%s\";";
    private static final String SECURITY_PROTOCOL = "security.protocol";
    private static final String SASL_JAAS_CONFIG = "sasl.jaas.config";
    private static final String SASL_MECHANISM = "sasl.mechanism";

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {

//        ParameterTool

        // Kafka Source Properties

        Properties properties = new Properties();

        // Kafka Source

        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setRuntimeMode(RuntimeExecutionMode.STREAMING);
//        environment.getConfig().setParallelism(1);

/*
        If checkpointing is not enabled, the “no restart” strategy is used.
        If checkpointing is activated and the restart strategy has not been configured,
        the fixed-delay strategy is used with Integer.MAX_VALUE restart attempts.
*/
        environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(5, TimeUnit.SECONDS)));
        KafkaSource<KafkaInput> kafkaSource = KafkaSource.<KafkaInput>builder()
                .setBootstrapServers("kafka-deployment.default.svc.cluster.local:9092")
//                .setBootstrapServers("kafka-deployment-controller-0.kafka-deployment-controller-headless.default.svc.cluster.local:9094")
                .setProperties(properties)
//                .setTopics("test-topic")
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
                        .name("DataSource")
                        .setParallelism(2);



       DataStream<String> apiCallDataStream = dataStream.map(new MockApiCallFunction())
               .name("API Call")
               .setParallelism(2);

       apiCallDataStream.addSink(new DiscardingSink<>())
               .setParallelism(2)
               .name("Sink");

        apiCallDataStream.print();
        //JSON parsing

        AtomicReference<Long> count = new AtomicReference<>(0L);

        environment.execute("Metrics test pipeline parallelism - 4");

    }
}
