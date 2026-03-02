import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.session.TableSessionBuilder;
import org.apache.iotdb.isession.SessionDataSet;

import javax.xml.transform.Source;
import java.util.Collections;


public class IoTDBSource implements SourceFunction<String> {
    private volatile boolean running = true;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {

        try(ITableSession session = new TableSessionBuilder()
                .nodeUrls(Collections.singletonList("127.0.0.1:6667"))
                .username("root")
                .password("root")
                .build()) {

            session.executeNonQueryStatement("USE factory_db");

            SessionDataSet dataSet =
                    session.executeQueryStatement("SELECT * FROM machines");

            while(dataSet.hasNext() && running) {
                ctx.collect(dataSet.next().toString());
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
