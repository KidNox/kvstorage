package kvstorage;

import java.util.Arrays;

final class ByteEntry {
    static final int LENGTH = 14;

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

    static boolean equals(byte[] b1, byte[] b2) {
        return Arrays.equals(b1, b2);
    }

    static long hash(byte[] bytes) {
        long result = 1;
        for (int i = 0; i < bytes.length; i++) {
            byte element = bytes[i];
            result = 31 * result + element;
        }
        return result;
    }

    static void writeInt(byte[] out, int offset, int value) {
        out[offset] = (byte) (value >> 24);
        out[offset + 1] = (byte) (value >> 16);
        out[offset + 2] = (byte) (value >> 8);
        out[offset + 3] = (byte) value;
    }

    static int readInt(byte[] in, int offset) {
        return ((in[offset] & 0xff) << 24)
                + ((in[offset + 1] & 0xff) << 16)
                + ((in[offset + 2] & 0xff) << 8)
                + (in[offset + 3] & 0xff);
    }

    static void writeLong(byte[] in, int offset, long v) {
        in[offset++] = (byte) ((v >>> 56) & 0xFF);
        in[offset++] = (byte) ((v >>> 48) & 0xFF);
        in[offset++] = (byte) ((v >>> 40) & 0xFF);
        in[offset++] = (byte) ((v >>> 32) & 0xFF);
        in[offset++] = (byte) ((v >>> 24) & 0xFF);
        in[offset++] = (byte) ((v >>> 16) & 0xFF);
        in[offset++] = (byte) ((v >>> 8) & 0xFF);
        in[offset] = (byte) (v & 0xFF);
    }

    static long readLong(byte[] in, int offset) {
        return ((long) (readInt(in, offset)) << 32) + (readInt(in, offset + 4) & 0xFFFFFFFFL);
    }

    static void writeShort(byte[] out, int offset, int v) {
        out[offset] = (byte) ((v & 0xFF00) >> 8);
        out[offset + 1] = (byte) (v & 0x00FF);
    }

    static int readShort(byte[] in, int offset) {
        return (short) ((in[offset] << 8) | (in[offset + 1] & 0xff));
    }

    static void copy(byte[] src, int pos, byte[] dest) {
        System.arraycopy(src, pos, dest, 0, dest.length);
    }

    static void fill(byte[] src, byte[] dest, int pos) {
        System.arraycopy(src, 0, dest, pos, src.length);
    }

    static void copy(byte[] src, int srcPos, byte[] dest, int destPos, int length) {
        System.arraycopy(src, srcPos, dest, destPos, length);
    }

    static byte[] subArray(byte[] src, int pos, int length) {
        byte[] result = new byte[length];
        System.arraycopy(src, pos, result, 0, length);
        return result;
    }
}
