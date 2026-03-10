import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

public class VoidSerializer implements SimpleVersionedSerializer<Void> {

    public static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(Void obj) throws IOException {
        return new byte[0];
    }

    @Override
    public Void deserialize(int version, byte[] serialized) throws IOException {
        return null;
    }
}
