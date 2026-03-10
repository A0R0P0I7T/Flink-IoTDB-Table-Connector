import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class IoTDBFlinkTestJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        env.fromSource(
                        new IoTDBSource(),
                        org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                        "IoTDB Source")
                .print();

        env.execute("IoTDB FLIP-27 Test Job");
    }
}