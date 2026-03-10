import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

public class FlinkJob {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        env.fromSource(
                new IoTDBSource(),
                WatermarkStrategy.noWatermarks(),
                "IoTDB Table Source"
                ).print();

        env.execute("IoTDB Flink Test");

    }
}
