package kvstorage;

import static kvstorage.ByteUtils.*;

final class ByteEntry {
    static final int LENGTH = 14;
    static final int KEY_MAX_LENGTH = Short.MAX_VALUE;

    final int position;
    final long hash;
    final int keyLength;
    final int valueLength;

    private byte[] key;
    private byte[] value;

    private int index = -1;

    ByteEntry(int position, long hash, int keyLength, int valueLength) {
        this.position = position;
        this.hash = hash;
        this.keyLength = keyLength;
        this.valueLength = valueLength;
    }

    static ByteEntry read(int position, byte[] bytes) {
        long hash = readLong(bytes, position);
        int keyLength = readShort(bytes, position + 8);
        int valueLength = readInt(bytes, position + 10);
        return new ByteEntry(position, hash, keyLength, valueLength);
    }

    int write(int position, byte[] out) {
        writeLong(out, position, hash);
        writeShort(out, position + 8, keyLength);
        writeInt(out, position + 10, valueLength);
        return position + LENGTH;
    }

    int entryLength() {
        return LENGTH + keyLength + valueLength;
    }

    int nextPosition() {
        return position + LENGTH + keyLength + valueLength;
    }

    int valuePosition() {
        return position + LENGTH + keyLength;
    }

    byte[] readKey(byte[] in) {
        return subArray(in, position + LENGTH, keyLength);
    }

    byte[] readValue(byte[] in) {
        return subArray(in, position + LENGTH + keyLength, valueLength);
    }

    byte[] getOrReadKey(byte[] in) {
        if (key == null) {
            key = readKey(in);
        }
        return key;
    }

    byte[] getOrReadValue(byte[] in) {
        if (value == null) {
            value = readValue(in);
        }
        return value;
    }

    void setValue(byte[] value) {
        this.value = value;
    }

    int getIndex() {
        return index;
    }

    void setIndex(int index) {
        this.index = index;
    }

    ByteEntry copyWithNewPosition(int newPosition) {
        ByteEntry entry = new ByteEntry(newPosition, hash, keyLength, valueLength);
        entry.key = key;
        entry.value = value;
        return entry;
    }

    ByteEntry copyWithNewValue(byte[] value) {
        ByteEntry entry = new ByteEntry(position, hash, keyLength, value.length);
        entry.key = key;
        entry.value = value;
        return entry;
    }


}
