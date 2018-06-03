package kvstorage;

import java.io.IOException;

class StorageSnapshot extends KVByteStorage {
    private final byte[] snapshot;

    StorageSnapshot(byte[] snapshot) {
        this.snapshot = snapshot;
    }

    @Override protected byte[] readBuffer() throws IOException {
        return snapshot;
    }

    @Override protected void writeBuffer(byte[] newBuffer) throws IOException {
        throw new UnsupportedOperationException("snapshot can't be modified");
    }
}
