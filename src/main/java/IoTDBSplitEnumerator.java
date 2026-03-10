import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

public class IoTDBSplitEnumerator implements SplitEnumerator<IoTDBSplit, Void> {

    private final SplitEnumeratorContext<IoTDBSplit> context;
    private final List<IoTDBSplit> remainingSplits = new ArrayList<>();

    // stores the context
    // Generates the splits using the createSplits() method
    public IoTDBSplitEnumerator(SplitEnumeratorContext<IoTDBSplit> context) {
        this.context = context;
    }

    public void createSplits() {
        long globalStart = 1700000000000L; // Hardcoded the values since this is a POC
        long globalEnd = 1800000000000L;

        int parallelism = context.currentParallelism(); // checks the number of parallel readers

        long range = globalEnd - globalStart; // Range of the total time window
        long splitSize = range / parallelism; // creates the split size for eg: 10000 / 4 = 2500 sized splits

        // Loops once per reader
        for(int i = 0; i<parallelism; i++){
            long start = globalStart + i * splitSize;
            long end = (i == parallelism - 1)
                    ? globalEnd // last split gets exact end to avoid rounding gaps
                    : start + splitSize;
            remainingSplits.add(
                    new IoTDBSplit("split-" +  i, start, end)
            );
        }
    }
    @Override
    public void start(){
        createSplits();
        System.out.println("Enumerator Started");
        // No-op for now since this is POC
    }

    // @param requesterHostname is not necessary for now as this is the POC build
    // it checks for split which is physically closest to the IoTDB data
    // reduces network transfer between flink and IoTDB
    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        if (!remainingSplits.isEmpty()) {
            System.out.println("Remaining splits: " + remainingSplits.size());
            IoTDBSplit split = remainingSplits.remove(0);
            System.out.println("Reader " + subtaskId + " requested split");
            context.assignSplit(split, subtaskId);
            System.out.println("Assigned split: " + split.splitId());
        }
        else{
            // no more splits left for this reader
            context.signalNoMoreSplits(subtaskId);
        }
    }

    // when a reader fails mid split this function adds the split back to the List
    @Override
    public void addSplitsBack(List<IoTDBSplit> splits, int subtaskId) {
        remainingSplits.addAll(splits);
    }


    @Override
    public void addReader(int subtaskId) {
        // Reader registered
    }

    // it snapshots the remaining splits in the form of bytes
    @Override
    public Void snapshotState(long checkpointId) {
        return null;
    }

    // no session being created so no logic inside it right now
    @Override
    public void close() throws IOException {

    }
}
