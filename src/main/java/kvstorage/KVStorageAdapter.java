package kvstorage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KVStorageAdapter extends Editor {
    private final KVStorage storage;
    private final ExceptionHandler exceptionHandler;

    public KVStorageAdapter(KVStorage storage) {
        this(storage, ex -> {
            throw new RuntimeException(ex);
        });
    }

    public KVStorageAdapter(KVStorage storage, ExceptionHandler exceptionHandler) {
        if (storage == null) throw new IllegalArgumentException();
        if (exceptionHandler == null) throw new IllegalArgumentException();
        this.storage = storage;
        this.exceptionHandler = exceptionHandler;
    }

    public final KVStorageAdapter snapshot() {
        try {
            KVStorage storage = new KVByteStorage(new ByteStorage.StorageSnapshot(this.storage.snapshot()));
            return new KVStorageAdapter(storage);
        } catch (Exception ex) {
            exceptionHandler.handleException(ex);
            throw new IllegalStateException("corrupted snapshot", ex);
        }
    }

    public final boolean getBoolean(String key) {
        return getBoolean(key, false);
    }

    public final boolean getBoolean(String key, boolean defValue) {
        byte[] value = getValueSafe(encodeKey(key));
        if (value == null) return defValue;
        return ByteUtils.readBoolean(value, 0);
    }

    public final int getInt(String key) {
        return getInt(key, 0);
    }

    public final int getInt(String key, int defValue) {
        byte[] value = getValueSafe(encodeKey(key));
        if (value == null) return defValue;
        return ByteUtils.readInt(value, 0);
    }

    public final long getLong(String key) {
        return getLong(key, 0);
    }

    public final long getLong(String key, long defValue) {
        byte[] value = getValueSafe(encodeKey(key));
        if (value == null) return defValue;
        return ByteUtils.readLong(value, 0);
    }

    public final double getDouble(String key) {
        return getDouble(key, 0);
    }

    public final double getDouble(String key, double defValue) {
        byte[] value = getValueSafe(encodeKey(key));
        if (value == null) return defValue;
        return ByteUtils.readDouble(value, 0);
    }

    public final String getString(String key) {
        byte[] value = getValueSafe(encodeKey(key));
        if (value == null) return null;
        return decodeValue(value);
    }

    public final byte[] getBytes(String key) {
        return getValueSafe(encodeKey(key));
    }

    public final byte[] getBytes(byte[] key) {
        return getValueSafe(key);
    }

    private byte[] getValueSafe(byte[] key) {
        try {
            return storage.get(key);
        } catch (Exception e) {
            exceptionHandler.handleException(e);
        }
        return null;
    }

    @Override public Editor put(byte[] key, byte[] value) {
        try {
            storage.put(key, value);
        } catch (Exception e) {
            exceptionHandler.handleException(e);
        }
        return this;
    }

    @Override public Editor remove(byte[] key) {
        try {
            storage.remove(key);
        } catch (Exception e) {
            exceptionHandler.handleException(e);
        }
        return this;
    }

    public final Editor clear() {
        try {
            storage.clear();
        } catch (Exception e) {
            exceptionHandler.handleException(e);
        }
        return this;
    }

    public final BulkEditor bulkInsert() {
        return new BulkEditor();
    }

    public class BulkEditor extends Editor<BulkEditor> {
        List<KVStorage.KeyValue> list = new ArrayList<>(4);

        @Override public BulkEditor put(byte[] key, byte[] value) {
            list.add(new KVStorage.KeyValue(key, value));
            return this;
        }

        @Override public BulkEditor remove(byte[] key) {
            list.add(new KVStorage.KeyValue(key, null));
            return this;
        }

        public final boolean commit() {
            try {
                storage.put(list.toArray(new KVStorage.KeyValue[list.size()]));
                return true;
            } catch (IOException e) {
                exceptionHandler.handleException(e);
            }
            return false;
        }
    }
}
