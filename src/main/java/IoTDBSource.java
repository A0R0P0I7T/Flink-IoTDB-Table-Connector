import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;

public class IoTDBSource implements Source<String, IoTDBSplit, Void> {

    //Finite source
    //Reads fixed time ranges
    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    // Each parallel subtask gets its own dedicated Reader instance
    // Flink calls this once per parallel slot on each TaskManager
    // readerContext provides access to metrics, split assignment notifications,
    // and the reader's subtask index (which parallel instance this is)
    @Override
    public SourceReader<String, IoTDBSplit> createReader(SourceReaderContext readerContext) {
        return new IoTDBSourceReader(readerContext);
    }

    // Creates a fresh SplitEnumerator when the job starts for the first time
    // Flink calls this on the JobManager during initial job submission
    // enumContext allows the enumerator to assign splits to readers,
    // check which readers are registered, and request splits back from failed readers
    @Override
    public SplitEnumerator<IoTDBSplit, Void> createEnumerator(SplitEnumeratorContext<IoTDBSplit> enumContext) {
        return new IoTDBSplitEnumerator(enumContext);
    }

    // Restores the SplitEnumerator from a previous checkpoint after job failure
    // Flink calls this on the JobManager during job recovery instead of createEnumerator
    // enumContext is the same as createEnumerator
    // checkpoint contains the previously saved enumerator state (Void here = no state)
    @Override
    public SplitEnumerator<IoTDBSplit, Void> restoreEnumerator(SplitEnumeratorContext<IoTDBSplit> enumContext, Void Checkpoint) {
        return new IoTDBSplitEnumerator(enumContext);
    }

    // Tells Flink how to serialize and deserialize IoTDBSplit objects
    // Required for sending splits from JobManager to TaskManagers over the network
    // and for saving split state to disk during checkpointing
    // SimpleVersionedSerializer handles versioning so old checkpoints
    // remain compatible with newer code
    @Override
    public SimpleVersionedSerializer<IoTDBSplit> getSplitSerializer() {
        return new IoTDBSplitSerializer();
    }

    // Tells Flink how to serialize and deserialize the enumerator's checkpoint state
    // Called during checkpointing to save enumerator state to disk
    // and during recovery to restore it
    // VoidSerializer means the enumerator has no state to save —
    // appropriate for a basic POC but in production it will track
    // pending splits, assigned splits, and finished splits
    @Override
    public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
        return new VoidSerializer();
    }
}
