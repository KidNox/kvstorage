package kvstorage;

import java.util.Random;

public class Utils {

    public static byte[] getRandomBytes(int length) {
        byte[] result = new byte[length];
        new Random().nextBytes(result);
        return result;
    }

    public static KVStorage.KeyValue[] getRandomKV(int length) {
        KVStorage.KeyValue[] result = new KVStorage.KeyValue[length];
        for (int i = 0; i < length; i++) {
            result[i] = new KVStorage.KeyValue(getRandomBytes(16), getRandomBytes(64));
        }
        return result;
    }
}
