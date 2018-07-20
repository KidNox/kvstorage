package kvstorage;

public abstract class Editor<T extends Editor> {
    public final T put(String key, boolean value) {
        byte[] out = new byte[1];
        ByteUtils.writeBoolean(out, 0, value);
        return put(encodeKey(key), out);
    }

    public final T putInt(String key, int value) {
        byte[] out = new byte[4];
        ByteUtils.writeInt(out, 0, value);
        return put(encodeKey(key), out);
    }

    public final T putLong(String key, long value) {
        byte[] out = new byte[8];
        ByteUtils.writeLong(out, 0, value);
        return put(encodeKey(key), out);
    }

    public final T putDouble(String key, double value) {
        byte[] out = new byte[8];
        ByteUtils.writeDouble(out, 0, value);
        return put(encodeKey(key), out);
    }

    public final T put(String key, String value) {
        return put(encodeKey(key), encodeValue(value));
    }

    public final T put(String key, byte[] value) {
        return put(encodeKey(key), value);
    }

    public final T remove(String key) {
        return remove(encodeKey(key));
    }

    public abstract T put(byte[] key, byte[] value);

    public abstract T remove(byte[] key);

    protected byte[] encodeKey(String key) {
        return ByteUtils.encodeString(key);
    }

    protected byte[] encodeValue(String value) {
        return ByteUtils.encodeString(value);
    }

    protected String decodeValue(byte[] value) {
        return ByteUtils.decodeString(value);
    }
}
