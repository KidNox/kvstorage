package kvstorage;

import java.io.IOException;

public interface KVStorage {
    
    void loadToBuffer() throws IOException;

    byte[] get(byte[] key) throws IOException;

    void put(byte[] key, byte[] value) throws IOException;

    void put(KeyValue... keyValues) throws IOException;

    boolean remove(byte[] key) throws IOException;

    void clear() throws IOException;
    
    byte[] snapshot() throws IOException;

    final class KeyValue {
        final byte[] key;
        final byte[] value;

        public KeyValue(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }
    }
}
