import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;


public class A4Application {

    public static void main(String[] args) throws Exception {
        // do not modify the structure of the command line
        String bootstrapServers = args[0];
        String appName = args[1];
        String studentTopic = args[2];
        String classroomTopic = args[3];
        String outputTopic = args[4];
        String stateStoreDir = args[5];

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateStoreDir);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        // add code here if you need any additional configuration options

        StreamsBuilder builder = new StreamsBuilder();

        // add code here
        //
        // ... = builder.stream(studentTopic);
        // ... = builder.stream(classroomTopic);
        // ...
        // ...to(outputTopic);
        KStream<String, String> classroomStream = builder.stream(classroomTopic);   // can't make it to <String, Integer> directly
        classroomStream.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

//        for test only: wired output
//        KStream<String, String> source = builder.stream(classroomTopic);
//        source
//                .flatMapValues(line -> Arrays.asList(line.toLowerCase().split("\\W+")))
//                .groupBy((key, word) -> word)
//                //.windowedBy(TimeWindows.of(TimeUnit.SECONDS.toMillis(10)))
//                .count()
//                //.filter((word, count) -> (count % 2 == 1))
//                .toStream()
//                // convert windowed key to ordinary key
//                //.map((key, value) -> KeyValue.pair(key.key(), value))
//                //.map((word, count) -> (count == null ? KeyValue.pair(word, -1L) : KeyValue.pair(word, count)))
//                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // this line initiates processing
        streams.start();

        // shutdown hook for Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
