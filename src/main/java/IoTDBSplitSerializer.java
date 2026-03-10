import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;

public class IoTDBSplitSerializer implements SimpleVersionedSerializer<IoTDBSplit> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(IoTDBSplit split) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeUTF(split.splitId());
        out.writeLong(split.getStartTime());
        out.writeLong(split.getEndTime());

        out.flush();
        return baos.toByteArray();
    }

    @Override
    public IoTDBSplit deserialize(int version, byte[] bytes) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputStream in = new DataInputStream(bais);

        String splitId = in.readUTF();
        long startTime = in.readLong();
        long endTime = in.readLong();

        return new IoTDBSplit(splitId,startTime,endTime);
    }
}
