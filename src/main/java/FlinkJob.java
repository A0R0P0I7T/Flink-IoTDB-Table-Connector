import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class FlinkJob {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        env.addSource(new IoTDBSource())
                .print();

        env.execute("IoTDB Flink Test");

    }
}
