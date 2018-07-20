package kvstorage;

import java.io.IOException;

public class KVStorageImpl implements KVStorage, ByteStorage {

    private KVStorage actualStorage;

    private byte[] initBuffer;

    byte[] newBuffer;

    KVStorageImpl() throws IOException {
        this(new byte[0]);
    }

    KVStorageImpl(byte[] initBuffer) throws IOException {
        this.initBuffer = initBuffer;
        init();
    }

    private void init() throws IOException {
        this.actualStorage = new KVByteStorage(this);
    }

    @Override public byte[] read() throws IOException {
        return initBuffer;
    }

    @Override public void write(byte[] bytes) throws IOException {
        newBuffer = bytes;
    }

    @Override public byte[] get(byte[] key) throws IOException {
        return actualStorage.get(key);
    }

    @Override public byte[] snapshot() throws IOException {
        return actualStorage.snapshot();
    }

    @Override public void put(byte[] key, byte[] value) throws IOException {
        actualStorage.put(key, value);
    }

    @Override public void put(KeyValue... keyValues) throws IOException {
        actualStorage.put(keyValues);
    }

    @Override public boolean remove(byte[] key) throws IOException {
        return actualStorage.remove(key);
    }

    @Override public void clear() throws IOException {
        actualStorage.clear();
    }
}
