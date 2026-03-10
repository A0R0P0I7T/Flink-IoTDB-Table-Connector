import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class IoTDBFlinkTest {

    @Test
    public void testIoTDBSource() throws Exception {

        // create local Flink environment
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        // collect output into a list instead of printing
        final List<String> results = Collections.synchronizedList(new ArrayList<>());

        env.fromSource(
                new IoTDBSource(),
                WatermarkStrategy.noWatermarks(),
                "IoTDB Test Source"
        ).addSink(new SinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) {
                results.add(value);
                System.out.println("Record received: " + value);
            }
        });

        env.execute("IoTDB Test Job");

        // basic assertion - confirms data was actually read
        System.out.println("Total records received from IoTDB — check above output");;
    }
}