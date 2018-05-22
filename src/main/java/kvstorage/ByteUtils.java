package kvstorage;

import java.nio.charset.Charset;
import java.util.Arrays;

final class ByteUtils {
    static final Charset UTF8 = Charset.forName("UTF-8");

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

    static void writeDouble(byte[] in, int offset, double v) {
        writeLong(in, offset, Double.doubleToLongBits(v));
    }

    static double readDouble(byte[] in, int offset) {
        return Double.longBitsToDouble(readLong(in, offset));
    }

    static void writeBoolean(byte[] in, int offset, boolean v) {
        in[offset] = (byte) (v ? 1 : 0);
    }

    static boolean readBoolean(byte[] in, int offset) {
        return in[offset] != 0;
    }

    static void writeShort(byte[] out, int offset, int v) {
        out[offset] = (byte) ((v & 0xFF00) >> 8);
        out[offset + 1] = (byte) (v & 0x00FF);
    }

    static int readShort(byte[] in, int offset) {
        return (short) ((in[offset] << 8) | (in[offset + 1] & 0xff));
    }

    static String decodeString(byte[] in) {
        return new String(in, UTF8);
    }

    static byte[] encodeString(String in) {
        return in.getBytes(UTF8);
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
