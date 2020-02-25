package io.pravega.flink;

import io.pravega.avro.AvroSampleSerializer;
import io.pravega.avro.Sample;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.TimeWindow;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.serialization.PravegaDeserializationSchema;
import io.pravega.connectors.flink.watermark.LowerBoundAssigner;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WatermarkReader {

    // Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(SimpleReader.class);

    // The application reads data from specified Pravega stream

    // Application parameters
    //   stream - default test-scope/eo-ingestion
    //   controller - default tcp://127.0.0.1:9090

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Simple Stream Reader...");

        // initialize the parameter utility tool in order to retrieve input parameters
        ParameterTool params = ParameterTool.fromArgs(args);
        PravegaConfig pravegaConfig = PravegaConfig
                .fromParams(params)
                .withDefaultScope(Constants.DEFAULT_SCOPE);

        // initialize the Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);

        // create the Pravega source to read a stream of text
        FlinkPravegaReader<Sample> source = FlinkPravegaReader.<Sample>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(Stream.of("test-scope", "bla"))
                .withDeserializationSchema(new PravegaDeserializationSchema<>(Sample.class, new AvroSampleSerializer()))
                .withTimestampAssigner(new LowerBoundAssigner<Sample>() {
                    private long currentLowerBound = 0;

                    @Override
                    public long extractTimestamp(Sample sample, long previousElementTimestamp) {
                        return currentLowerBound;
                    }

                    @Override
                    public Watermark getWatermark(TimeWindow timeWindow) {
                        this.currentLowerBound = timeWindow.getLowerTimeBound();
                        return super.getWatermark(timeWindow);
                    }
                })
                .build();

        // simply read events
        DataStream<Sample> sampleStream = env.addSource(source)
                .name("Pravega Stream")
                .keyBy(0)
                .timeWindow(Time.seconds(10))
                .sum(1);

        // create an output sink to print to stdout for verification
        sampleStream.addSink(new DiscardingSink<>()).name("Discarding output");

        // execute within the Flink environment
        env.execute("SimpleReader");

        LOG.info("Ending SimpleReader...");
    }
}
