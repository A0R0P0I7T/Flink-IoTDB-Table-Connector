import org.apache.flink.api.connector.source.SourceSplit;

public class IoTDBSplit implements SourceSplit {
    private final String splitId;
    private final long startTime;
    private final long endTime;

    // Receives the appropriate splitId, startTime, endTime
    // To create the proper splits
    public IoTDBSplit(String splitId, long startTime, long endTime) {
        this.splitId = splitId;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    // Getters for the parameters
    @Override
    public String splitId() {
        return splitId;
    }
    public long getStartTime() {
        return startTime;
    }
    public long getEndTime() {
        return endTime;
    }

    @Override
    public String toString(){
        return "IoTDBSplit{" +
                "splidId='" + splitId + '\''
                + ", startTime='" + startTime
                + ", endTime='" + endTime +
                '}';
    }
}


