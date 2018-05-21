package kvstorage;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static kvstorage.Utils.getRandomBytes;
import static kvstorage.Utils.getRandomKV;
import static org.junit.Assert.assertArrayEquals;

public class KVFileStorageTest {
    @Rule public TemporaryFolder folder = new TemporaryFolder();

    private File file;

    @Before public void setUp() throws Exception {
        File parent = folder.getRoot();
        file = new File(parent, "storage");
    }

    @Test public void testInitNonEmptyFile() throws IOException {
        KVStorage storage = new KVFileStorage(file);
        byte[] key1 = getRandomBytes(16);
        byte[] value1 = getRandomBytes(128);
        byte[] key2 = getRandomBytes(33);
        byte[] value2 = getRandomBytes(99);
        storage.put(key1, value1);
        storage.put(key2, value2);

        storage = new KVFileStorage(file);
        assertArrayEquals(value1, storage.get(key1));
        assertArrayEquals(value2, storage.get(key2));
    }

    @Test public void testInitNonEmptyBuffer() throws IOException {
        KVStorage storage = new KVFileStorage(file);
        byte[] key1 = getRandomBytes(13);
        byte[] value1 = getRandomBytes(200);
        byte[] key2 = getRandomBytes(33);
        byte[] value2 = getRandomBytes(99);
        byte[] key3 = getRandomBytes(34);
        byte[] value3 = getRandomBytes(99);
        storage.put(key1, value1);
        storage.put(key2, value2);
        storage = new KVFileStorage(file);
        assertArrayEquals(value1, storage.get(key1));
        assertArrayEquals(value2, storage.get(key2));
        storage.put(key3, value3);
        storage = new KVFileStorage(file);
        assertArrayEquals(value1, storage.get(key1));
        assertArrayEquals(value2, storage.get(key2));
        assertArrayEquals(value3, storage.get(key3));
    }

    @Test public void testKeyValuesReplace() throws IOException {
        KVStorage storage = new KVFileStorage(file);
        KVStorage.KeyValue[] keyValues = getRandomKV(12);
        KVStorage.KeyValue tested = keyValues[8];
        KVStorage.KeyValue tested2 = keyValues[0];
        KVStorage.KeyValue tested3 = keyValues[11];
        storage.put(keyValues);
        storage = new KVFileStorage(file);
        assertArrayEquals(tested.value, storage.get(tested.key));
        assertArrayEquals(tested2.value, storage.get(tested2.key));
        keyValues = getRandomKV(5);
        keyValues[0] = new KVStorage.KeyValue(tested.key, getRandomBytes(14));
        keyValues[2] = new KVStorage.KeyValue(tested2.key, getRandomBytes(42));
        keyValues[4] = new KVStorage.KeyValue(tested3.key, getRandomBytes(46));
        storage.put(keyValues);
        storage = new KVFileStorage(file);
        assertArrayEquals(keyValues[0].value, storage.get(tested.key));
        assertArrayEquals(keyValues[2].value, storage.get(tested2.key));
        assertArrayEquals(keyValues[4].value, storage.get(tested3.key));
    }
}
