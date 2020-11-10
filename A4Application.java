import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.nio.charset.IllegalCharsetNameException;
import java.util.Arrays;
import java.util.Properties;

/* References:
 * 1. https://kafka.apache.org/24/documentation/streams/core-concepts.html
 * 2. https://kafka.apache.org/24/documentation/streams/developer-guide/dsl-api.html
 */
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

        KStream<String, String> studentStream = builder.stream(studentTopic);   // Consumed.with(Serdes.String(), Serdes.String())
        KStream<String, String> classroomStream = builder.stream(classroomTopic);

        // key: student, value: classroom
        KTable<String, String> studentLocation = studentStream.groupByKey().reduce((aggValue, newValue) -> newValue);
        // key: classroom, value: capacity
        KTable<String, String> classroomCapacity = classroomStream.groupByKey().reduce((aggValue, newValue) -> newValue);

        // key: classroom, value: occupancy
        KTable<String, Long> classroomOccupancy = studentLocation
                .groupBy((student, classroom) ->  KeyValue.pair(classroom, student))
                .count();

        // key: classroom, value: (occupancy:capacity)
        // inner join: some room only exists in occupancy because its capacity is unlimited, ignore it
        KTable<String, String> classroomStatus = classroomOccupancy
                .join(classroomCapacity, (occupancy, capacity) -> occupancy.toString() + ":" + capacity);
        // omit the third arg in join():
        // Joined.with(Serdes.String(), Serdes.String(), Serdes.String())  // classroom type, occupancy type, capacity type

        // key: classroom, value: occupancy or OK (under certain circumstances)
        // value type set to String instead of Long because of "OK"
        KTable<String, String> output = classroomStatus.toStream()  // use toStream() to use groupByKey()
                .groupByKey()   // key is classroom
                .aggregate(
                        () -> null, /* initializer */
                        (aggKey, newValue, oldValue) -> {
                            String[] data = newValue.split(":");
                            int occupancy = Integer.parseInt(data[0]);
                            int capacity = Integer.parseInt(data[1]);
                            if (occupancy > capacity) {
                                return String.valueOf(occupancy);
                            } else {
                                if (oldValue != null) { // occupancy exceeded capacity previously
                                    return "OK";
                                } else {
                                    return null;
                                }
                            }
                        }
                );
        // omit the third arg in aggregate()
        // Materialized.as("aggregated-stream-store") /* state store name */
        //              .withValueSerde(Serdes.String()); /* serde for aggregate value */

        output.toStream()
                .filter((key, value) -> value != null)
                .to(outputTopic);
        // Produced with() is same as default in config, could omit
        // output.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // this line initiates processing
        streams.start();

        // shutdown hook for Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
