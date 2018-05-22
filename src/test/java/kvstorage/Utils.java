package kvstorage;

import java.io.*;
import java.lang.reflect.Field;
import java.util.Random;

public class Utils {

    public static byte[] getRandomBytes(int length) {
        byte[] result = new byte[length];
        new Random().nextBytes(result);
        return result;
    }

    public static KVByteStorage.KeyValue[] getRandomKV(int length) {
        KVByteStorage.KeyValue[] result = new KVByteStorage.KeyValue[length];
        for (int i = 0; i < length; i++) {
            result[i] = new KVByteStorage.KeyValue(getRandomBytes(16), getRandomBytes(64));
        }
        return result;
    }

    public static BrokenStreamWrapper brokenOutput() {
        BrokenStreamWrapper streamWrapper = new BrokenStreamWrapper();
        streamWrapper.brokenOutput = true;
        return streamWrapper;
    }

    public static class BrokenStreamWrapper implements StreamWrapper {
        boolean brokenInput;
        boolean brokenOutput;

        @Override public InputStream inputStream(InputStream is) throws IOException {
            if (brokenInput) {
                throw new IOException("brokenInputStream");
            }
            return is;
        }

        @Override public OutputStream outputStream(OutputStream os) throws IOException {
            if (brokenOutput) {
                throw new IOException("brokenOutputStream");
            }
            return os;
        }
    }

    public static ExceptionHandlerImpl createExceptionHandler() {
        return new ExceptionHandlerImpl();
    }

    public static class ExceptionHandlerImpl implements ExceptionHandler {
        Exception exception;

        @Override public void handleException(Exception ex) {
            exception = ex;
        }
    }

    public static byte[] getBuffer(KVStorage storage) {
        if (storage instanceof KVByteStorage) {
            return (byte[]) readField(storage, KVByteStorage.class, "buffer");
        } else {
            throw new IllegalArgumentException();
        }
    }

    private static Object readField(Object instance, Class clazz, String fieldName) {
        try {
            Field f = clazz.getDeclaredField(fieldName);
            f.setAccessible(true);
            return f.get(instance);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
