import org.apache.iotdb.isession.ISessionDataSet;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.session.TableSessionBuilder;
import org.apache.iotdb.isession.SessionDataSet;

import java.util.Collections;

public class IoTDBTest {

    public static void main(String[] args) throws Exception{

        try(ITableSession session = new TableSessionBuilder()
                .nodeUrls(Collections.singletonList("127.0.0.1:6667"))
                .username("root")
                .password("root")
                .build()) {
            System.out.println("Connected to IoTDB");

            session.executeNonQueryStatement("USE factory_db");

            SessionDataSet dataSet =
                    session.executeQueryStatement("SELECT * FROM machines");

            while (dataSet.hasNext()) {
                System.out.println(dataSet.next());
            }

            System.out.println("Query Finished");
        }
    }
}
