import org.apache.flink.api.connector.source.SourceReader; //defines all methods required to implement
import org.apache.flink.api.connector.source.SourceReaderContext;//reader's toolbox-allows communication with enumerator
import org.apache.flink.api.connector.source.ReaderOutput; // conveyor belt -push record into this flink routes them
import org.apache.flink.core.io.InputStatus;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.session.TableSessionBuilder;
import org.apache.iotdb.isession.SessionDataSet;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

public class IoTDBSourceReader implements SourceReader<String, IoTDBSplit> {

    private final SourceReaderContext context;

    private final Queue<IoTDBSplit> splitsQueue = new ArrayDeque<>();

    private ITableSession session;

    // The active lazy cursor for current split's query
    // null means no active query, need to open one for new split
    private SessionDataSet dataSet;

    private IoTDBSplit currentSplit;

    // Flag set when enumerator signals no more splits will ever come
    private boolean noMoreSplits = false;

    public IoTDBSourceReader(SourceReaderContext context) {
        this.context = context;
    }

    @Override
    public void start() {

        try {
            System.out.println("Reader Started");
            session = new TableSessionBuilder()
                    .nodeUrls(Collections.singletonList("127.0.0.1:6667"))
                    .username("root")
                    .password("root")
                    .build();

            session.executeNonQueryStatement("USE factory_db");

        } catch (Exception e) {
            throw new RuntimeException("Failed to connect to IoTDB", e);
        }
        context.sendSplitRequest();
    }

    // Must return quickly - emits ONE row per call
    @Override
    public InputStatus pollNext(ReaderOutput<String> output) throws Exception {

        // Step 1: if no active split, try to get one from queue
        if (currentSplit == null) {
            currentSplit = splitsQueue.poll();

            // queue was empty
            if (currentSplit == null) {
                // if enumerator said no more splits AND queue is empty
                // this reader is completely done
                if (noMoreSplits) {
                    return InputStatus.END_OF_INPUT;
                }
                // otherwise wait for enumerator to assign a split
                return InputStatus.NOTHING_AVAILABLE;
            }

            // Step 2: new split acquired, open IoTDB query cursor
            String query = "SELECT * FROM machines WHERE time >= "
                    + currentSplit.getStartTime()
                    + " AND time < "
                    + currentSplit.getEndTime();

            dataSet = session.executeQueryStatement(query);
        }

        // Step 3: emit ONE row from current dataset
        if (dataSet.hasNext()) {
            output.collect(dataSet.next().toString());
            return InputStatus.MORE_AVAILABLE; // come back immediately for next row
        }

        // Step 4: dataset exhausted - split fully read
        // Release server-side cursor to free IoTDB connection pool
        dataSet.closeOperationHandle();
        dataSet = null;
        currentSplit = null;

        // Ask enumerator for next split
        context.sendSplitRequest();

        // wait until next split arrives via addSplits()
        return InputStatus.NOTHING_AVAILABLE;
    }



    // It stores current split progress with current offset so that
    // it does not have to start form the startTime again
    @Override
    public List<IoTDBSplit> snapshotState(long checkpointId) {
        if (currentSplit == null) {
            return Collections.emptyList();
        }

        return Collections.singletonList(
                new IoTDBSplit(
                        currentSplit.splitId(),
                        currentSplit.getStartTime(),
                        currentSplit.getEndTime()
                )
        );
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void addSplits(List<IoTDBSplit> splits) {
        splitsQueue.addAll(splits); // adds the remainingSplits list from SplitEnumerator to Queue
    }

    @Override
    public void notifyNoMoreSplits() {
        noMoreSplits = true;
    }

    @Override
    public void close() throws Exception {

        if (dataSet != null) {
            dataSet.closeOperationHandle();;
        }

        if (session != null) {
            session.close(); // release IoTDB connection after proper Sessions are created
        }
    }
}
