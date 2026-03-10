import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.session.TableSessionBuilder;
import java.util.Collections;

/**
 * Run this class once before running FlinkJob or IoTDBFlinkTest.
 * It creates the required database, table, and inserts sample sensor data
 * into your local IoTDB instance so the connector has data to read.
 *
 * Prerequisites:
 * - IoTDB 2.X running on localhost:6667
 * - Default credentials (root/root)
 */
public class DataSetup {

    public static void main(String[] args) throws Exception {

        ITableSession session = new TableSessionBuilder()
                .nodeUrls(Collections.singletonList("127.0.0.1:6667"))
                .username("root")
                .password("root")
                .build();

        System.out.println("Connected to IoTDB successfully.");

        // create database
        try {
            session.executeNonQueryStatement("CREATE DATABASE factory_db");
            System.out.println("Database factory_db created.");
        } catch (Exception e) {
            System.out.println("Database factory_db already exists, skipping.");
        }

        // select database
        session.executeNonQueryStatement("USE factory_db");

        // create table with TAG and FIELD columns
        // TAG columns are indexed and used for filtering (device_id, location)
        // FIELD columns carry the actual time-series measurements
        try {
            session.executeNonQueryStatement(
                    "CREATE TABLE machines (" +
                            "    device_id STRING TAG," +
                            "    location  STRING TAG," +
                            "    temperature FLOAT FIELD," +
                            "    active    BOOLEAN FIELD" +
                            ")"
            );
            System.out.println("Table machines created.");
        } catch (Exception e) {
            System.out.println("Table machines already exists, skipping.");
        }

        // insert sample sensor data across the configured time range
        // globalStart = 1700000000000L, globalEnd = 1800000000000L
        // inserting 6 records spread across the range
        String[] inserts = {
                "INSERT INTO machines(time, device_id, location, temperature, active) " +
                        "VALUES (1700000000000, 'd1', 'office', 22.5, true)",
                "INSERT INTO machines(time, device_id, location, temperature, active) " +
                        "VALUES (1710000000000, 'd2', 'warehouse', 30.5, false)",
                "INSERT INTO machines(time, device_id, location, temperature, active) " +
                        "VALUES (1720000000000, 'd1', 'office', 24.1, true)",
                "INSERT INTO machines(time, device_id, location, temperature, active) " +
                        "VALUES (1730000000000, 'd3', 'warehouse', 42.5, true)",
                "INSERT INTO machines(time, device_id, location, temperature, active) " +
                        "VALUES (1750000000000, 'd2', 'office', 19.8, false)",
                "INSERT INTO machines(time, device_id, location, temperature, active) " +
                        "VALUES (1780000000000, 'd1', 'warehouse', 35.0, true)"
        };

        for (String insert : inserts) {
            session.executeNonQueryStatement(insert);
        }

        System.out.println("6 sample records inserted successfully.");

        // verify data was inserted
        var result = session.executeQueryStatement("SELECT * FROM machines");
        System.out.println("\nVerification — records in machines table:");
        while (result.hasNext()) {
            System.out.println("  " + result.next().toString());
        }
        result.closeOperationHandle();

        session.close();
        System.out.println("\nSetup complete. You can now run FlinkJob or IoTDBFlinkTest.");
    }
}