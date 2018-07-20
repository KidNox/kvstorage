package kvstorage;

import java.io.File;
import java.io.IOException;

public class StorageFactory {
    private ExceptionHandler exceptionHandler;
    private StreamWrapper streamWrapper;
    private boolean strictWriteMode;

    public StorageFactory setDafaultExceptionHandler(ExceptionHandler handler) {
        exceptionHandler = handler;
        return this;
    }

    public StorageFactory setDefaultStreamWrapper(StreamWrapper wrapper) {
        streamWrapper = wrapper;
        return this;
    }

    public StorageFactory setStrictWriteMode(boolean strictWrite) {
        strictWriteMode = strictWrite;
        return this;
    }

    public KVStorageAdapter newStorage(File file) throws IOException {
        return new KVStorageAdapter(new StorageProxy(() ->
                new KVByteStorage(new ByteFileStorage(file, strictWriteMode, streamWrapper))),
                exceptionHandler);
    }

    private interface InternalFactory {
        KVStorage create() throws IOException;
    }

    private class StorageProxy implements KVStorage {
        final InternalFactory factory;

        KVStorage actualStorage;

        private StorageProxy(InternalFactory factory) {
            this.factory = factory;
        }

        private KVStorage getStorage() throws IOException {
            KVStorage local = actualStorage;
            if (local == null) {
                synchronized (this) {
                    if (actualStorage == null) {
                        actualStorage = factory.create();
                    }
                    local = actualStorage;
                }
            }
            return local;
        }

        @Override public byte[] get(byte[] key) throws IOException {
            return getStorage().get(key);
        }

        @Override public byte[] snapshot() throws IOException {
            return getStorage().snapshot();
        }

        @Override public void put(byte[] key, byte[] value) throws IOException {
            getStorage().put(key, value);
        }

        @Override public void put(KeyValue... keyValues) throws IOException {
            getStorage().put(keyValues);
        }

        @Override public boolean remove(byte[] key) throws IOException {
            return getStorage().remove(key);
        }

        @Override public void clear() throws IOException {
            getStorage().clear();
        }
    }
}
