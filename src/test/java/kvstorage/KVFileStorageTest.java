package kvstorage;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static kvstorage.Utils.getRandomBytes;
import static kvstorage.Utils.getRandomKV;
import static org.junit.Assert.*;

public class KVFileStorageTest {
    @Rule public TemporaryFolder folder = new TemporaryFolder();

    private File file;
    private KVByteStorage storage;

    @Before public void setUp() throws Exception {
        File parent = folder.getRoot();
        file = new File(parent, "storage");
        storage = new KVFileStorage(file);
    }

    @Test public void testInitNonEmptyFile() throws IOException {
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
        KVByteStorage.KeyValue[] keyValues = getRandomKV(12);
        KVByteStorage.KeyValue tested = keyValues[8];
        KVByteStorage.KeyValue tested2 = keyValues[0];
        KVByteStorage.KeyValue tested3 = keyValues[11];
        storage.put(keyValues);
        storage = new KVFileStorage(file);
        assertArrayEquals(tested.value, storage.get(tested.key));
        assertArrayEquals(tested2.value, storage.get(tested2.key));
        keyValues = getRandomKV(5);
        keyValues[0] = new KVByteStorage.KeyValue(tested.key, getRandomBytes(14));
        keyValues[2] = new KVByteStorage.KeyValue(tested2.key, getRandomBytes(42));
        keyValues[4] = new KVByteStorage.KeyValue(tested3.key, getRandomBytes(46));
        storage.put(keyValues);
        storage = new KVFileStorage(file);
        assertArrayEquals(keyValues[0].value, storage.get(tested.key));
        assertArrayEquals(keyValues[2].value, storage.get(tested2.key));
        assertArrayEquals(keyValues[4].value, storage.get(tested3.key));
    }

    @Test public void testBrokenOutput() throws IOException {
        byte[] key1 = getRandomBytes(16);
        byte[] value1 = getRandomBytes(128);
        byte[] key2 = getRandomBytes(33);
        byte[] value2 = getRandomBytes(99);
        storage.put(key1, value1);
        storage = new KVFileStorage(file, Utils.brokenOutput());
        try {
            storage.put(key2, value2);
            fail();
        } catch (IOException ignored) {//expected
        }
        storage = new KVFileStorage(file);//must restore from backup
        assertArrayEquals(value1, storage.get(key1));
        assertNull(storage.get(key2));
        storage.put(key2, value2);
        assertArrayEquals(value2, storage.get(key2));
    }

    @Test public void testBrokenOutput2() throws IOException {
        byte[] key1 = getRandomBytes(16);
        byte[] value1 = getRandomBytes(128);
        byte[] key2 = getRandomBytes(33);
        byte[] value2 = getRandomBytes(42);
        storage.put(key1, value1);
        storage = new KVFileStorage(file, Utils.brokenOutput());
        try {
            storage.put(key2, getRandomBytes(66));
            fail();
        } catch (IOException ignored) {//expected
        }
        storage = new KVFileStorage(file);//must restore from backup
        assertArrayEquals(value1, storage.get(key1));
        assertNull(storage.get(key2));
        storage = new KVFileStorage(file, Utils.brokenOutput());//check backup on permanent fail
        try {
            storage.put(key2, getRandomBytes(66));
            fail();
        } catch (IOException ignored) {//expected
        }
        storage = new KVFileStorage(file);//must restore from backup
        assertArrayEquals(value1, storage.get(key1));
        storage.put(key2, value2);
        storage = new KVFileStorage(file);
        assertArrayEquals(value1, storage.get(key1));
        assertArrayEquals(value2, storage.get(key2));
    }

    @Test public void testNotCorruptBufferOnError() throws IOException {
        byte[] key1 = getRandomBytes(17);
        byte[] value1 = getRandomBytes(23);
        byte[] key2 = getRandomBytes(27);
        byte[] value2 = getRandomBytes(37);
        Utils.BrokenStreamWrapper streamWrapper = new Utils.BrokenStreamWrapper();
        storage = new KVFileStorage(file, streamWrapper);
        storage.put(key1, value1);
        byte[] internalBuffer = Utils.getBuffer(storage);
        streamWrapper.brokenOutput = true;
        try {
            storage.put(key2, value2);
        } catch (Exception ignored) {
        }
        assertTrue(internalBuffer == Utils.getBuffer(storage));
        assertNull(storage.get(key2));
        assertArrayEquals(value1, storage.get(key1));
    }

    @Test public void testExceptionHandler() {
        Utils.ExceptionHandlerImpl exceptionHandler = Utils.createExceptionHandler();
        KVStorageAdapter adapter = new KVStorageAdapter(storage = new KVFileStorage(file, Utils.brokenOutput()), exceptionHandler);
        adapter.put("a", "b");
        assertNotNull(exceptionHandler.exception);
        assertEquals(0, Utils.getBuffer(storage).length);
    }
}
