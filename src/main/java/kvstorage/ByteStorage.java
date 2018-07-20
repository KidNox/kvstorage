package kvstorage;

import java.io.IOException;

interface ByteStorage {
    byte[] read() throws IOException;

    void write(byte[] bytes) throws IOException;

    class StorageSnapshot implements ByteStorage {
        final byte[] data;

        StorageSnapshot(byte[] data) {
            this.data = data;
        }

        @Override public byte[] read() throws IOException {
            return data;
        }

        @Override public void write(byte[] bytes) throws IOException {
            throw new UnsupportedOperationException("snapshot can't be modified");
        }
    }
}
