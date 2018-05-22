package kvstorage;

import java.io.IOException;

public class KVStorageImpl extends KVByteStorage {

    private final byte[] initBuffer;

    byte[] newBuffer;

    public KVStorageImpl() {
        this(new byte[0]);
    }

    public KVStorageImpl(byte[] initBuffer) {
        this.initBuffer = initBuffer;
    }

    @Override protected byte[] readBuffer() throws IOException {
        return initBuffer;
    }

    @Override protected void writeNewBuffer(byte[] newBuffer) throws IOException {
        super.writeNewBuffer(newBuffer);
        this.newBuffer = newBuffer;
    }
}
