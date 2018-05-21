package kvstorage;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static kvstorage.Utils.getRandomBytes;
import static kvstorage.Utils.getRandomKV;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;

public class KVStorageTest {
    private KVStorageImpl kvStorage;

    @Before public void setUp() {
        kvStorage = new KVStorageImpl();
    }

    @Test public void testEmptyBuffer() throws IOException {
        kvStorage.ensureBufferInitialized();
    }

    @Test public void testEmptyClear() throws IOException {
        kvStorage.clear();
    }

    @Test public void testEmptyPut() throws IOException {
        kvStorage.put(getRandomBytes(10), getRandomBytes(30));
    }

    @Test public void testEmptyGet() throws IOException {
        assertNull(kvStorage.get(getRandomBytes(12)));
    }

    @Test public void testGet() throws IOException {
        byte[] key = getRandomBytes(16);
        byte[] value = getRandomBytes(100);
        kvStorage.put(key, value);
        assertArrayEquals(value, kvStorage.get(key));
    }

    @Test public void testGetCantFind() throws IOException {
        byte[] key = getRandomBytes(16);
        byte[] value = getRandomBytes(100);
        kvStorage.put(key, value);
        key[2] = (byte) (key[2] + 1);
        assertNull(kvStorage.get(key));
    }

    @Test public void testAdd() throws IOException {
        byte[] key1 = getRandomBytes(42);
        byte[] value1 = getRandomBytes(66);
        byte[] key2 = getRandomBytes(11);
        byte[] value2 = getRandomBytes(99);
        kvStorage.put(key1, value1);
        kvStorage.put(key2, value2);

        assertArrayEquals(value2, kvStorage.get(key2));
        assertArrayEquals(value1, kvStorage.get(key1));
    }

    @Test public void testReplace() throws IOException {
        byte[] key1 = getRandomBytes(30);
        byte[] value1 = getRandomBytes(14);
        byte[] value2 = getRandomBytes(88);
        kvStorage.put(key1, value1);
        assertArrayEquals(value1, kvStorage.get(key1));
        kvStorage.put(key1, value2);
        assertArrayEquals(value2, kvStorage.get(key1));
    }

    @Test public void testReplaceSameLength() throws IOException {
        byte[] key1 = getRandomBytes(16);
        byte[] value1 = getRandomBytes(200);
        byte[] value2 = getRandomBytes(200);
        kvStorage.put(key1, value1);
        kvStorage.put(key1, value2);
        assertArrayEquals(value2, kvStorage.get(key1));
    }

    @Test public void testReplaceAndAdd() throws IOException {
        byte[] key1 = getRandomBytes(71);
        byte[] value1 = getRandomBytes(123);
        byte[] value2 = getRandomBytes(324);
        kvStorage.put(key1, value1);
        kvStorage.put(key1, value2);
        byte[] key3 = getRandomBytes(41);
        byte[] value3 = getRandomBytes(71);
        kvStorage.put(key3, value3);
        assertArrayEquals(value2, kvStorage.get(key1));
        assertArrayEquals(value3, kvStorage.get(key3));
    }

    @Test public void testReplaceSameLengthAndAdd() throws IOException {
        byte[] key1 = getRandomBytes(9);
        byte[] value1 = getRandomBytes(87);
        byte[] value2 = getRandomBytes(87);
        kvStorage.put(key1, value1);
        kvStorage.put(key1, value2);
        assertArrayEquals(value2, kvStorage.get(key1));
        byte[] key3 = getRandomBytes(41);
        byte[] value3 = getRandomBytes(88);
        kvStorage.put(key3, value3);
        assertArrayEquals(value2, kvStorage.get(key1));
        assertArrayEquals(value3, kvStorage.get(key3));
    }

    @Test public void testReplace2() throws IOException {
        byte[] key1 = getRandomBytes(16);
        byte[] value1 = getRandomBytes(66);
        byte[] key2 = getRandomBytes(11);
        byte[] value2 = getRandomBytes(99);
        byte[] key3 = getRandomBytes(42);
        byte[] value3 = getRandomBytes(43);
        byte[] key4 = getRandomBytes(32);
        byte[] value4 = getRandomBytes(32);
        byte[] value5 = getRandomBytes(111);
        byte[] value6 = getRandomBytes(87);

        kvStorage.put(key1, value1);
        kvStorage.put(key2, value2);
        kvStorage.put(key3, value3);
        kvStorage.put(key4, value4);

        kvStorage.put(key2, value6);
        kvStorage.put(key1, value5);

        assertArrayEquals(value5, kvStorage.get(key1));
        assertArrayEquals(value6, kvStorage.get(key2));
        assertArrayEquals(value3, kvStorage.get(key3));
        assertArrayEquals(value4, kvStorage.get(key4));
    }

    @Test public void testReplaceSameLength2() throws IOException {
        byte[] key1 = getRandomBytes(16);
        byte[] value1 = getRandomBytes(66);
        byte[] key2 = getRandomBytes(11);
        byte[] value2 = getRandomBytes(99);
        byte[] key3 = getRandomBytes(42);
        byte[] value3 = getRandomBytes(43);
        byte[] key4 = getRandomBytes(32);
        byte[] value4 = getRandomBytes(32);
        byte[] value5 = getRandomBytes(99);

        kvStorage.put(key1, value1);
        kvStorage.put(key2, value2);
        kvStorage.put(key3, value3);
        kvStorage.put(key4, value4);

        kvStorage.put(key2, value5);
        assertArrayEquals(value5, kvStorage.get(key2));
    }

    @Test public void testReplace3() throws IOException {
        byte[] key1 = getRandomBytes(16);
        byte[] value1 = getRandomBytes(128);
        byte[] key2 = getRandomBytes(33);
        byte[] value2 = getRandomBytes(32);
        byte[] value3 = getRandomBytes(74);
        kvStorage.put(key1, value1);
        kvStorage.put(key2, value2);
        kvStorage.put(key1, value3);
        assertArrayEquals(value2, kvStorage.get(key2));
        assertArrayEquals(value3, kvStorage.get(key1));
    }

    @Test public void testReplaceAndAdd2() throws IOException {
        byte[] key1 = getRandomBytes(16);
        byte[] value1 = getRandomBytes(66);
        byte[] key2 = getRandomBytes(11);
        byte[] value2 = getRandomBytes(99);
        byte[] key3 = getRandomBytes(42);
        byte[] value3 = getRandomBytes(43);
        byte[] key4 = getRandomBytes(32);
        byte[] value4 = getRandomBytes(29);
        byte[] value5 = getRandomBytes(111);
        byte[] value6 = getRandomBytes(87);
        byte[] value7 = getRandomBytes(49);
        kvStorage.put(key1, value1);
        kvStorage.put(key2, value2);
        kvStorage.put(key3, value3);
        kvStorage.put(key2, value6);
        kvStorage.put(key1, value5);
        assertArrayEquals(value5, kvStorage.get(key1));
        assertArrayEquals(value6, kvStorage.get(key2));
        assertArrayEquals(value3, kvStorage.get(key3));
        kvStorage.put(key3, value7);
        kvStorage.put(key1, value6);
        kvStorage.put(key4, value4);
        assertArrayEquals(value4, kvStorage.get(key4));
        assertArrayEquals(value6, kvStorage.get(key1));
        assertArrayEquals(value7, kvStorage.get(key3));
    }

    @Test public void testKeyValues() throws IOException {
        KVStorage.KeyValue[] keyValues = getRandomKV(10);
        KVStorage.KeyValue tested = keyValues[7];
        kvStorage.put(keyValues);
        assertArrayEquals(tested.value, kvStorage.get(tested.key));
    }

    @Test public void testKeyValuesReplace() throws IOException {
        KVStorage.KeyValue[] keyValues = getRandomKV(12);
        KVStorage.KeyValue tested = keyValues[8];
        KVStorage.KeyValue tested2 = keyValues[0];
        KVStorage.KeyValue tested3 = keyValues[11];
        kvStorage.put(keyValues);
        assertArrayEquals(tested.value, kvStorage.get(tested.key));
        assertArrayEquals(tested2.value, kvStorage.get(tested2.key));
        keyValues = getRandomKV(5);
        keyValues[0] = new KVStorage.KeyValue(tested.key, getRandomBytes(14));
        keyValues[2] = new KVStorage.KeyValue(tested2.key, getRandomBytes(42));
        keyValues[4] = new KVStorage.KeyValue(tested3.key, getRandomBytes(46));
        kvStorage.put(keyValues);
        assertArrayEquals(keyValues[0].value, kvStorage.get(tested.key));
        assertArrayEquals(keyValues[2].value, kvStorage.get(tested2.key));
        assertArrayEquals(keyValues[4].value, kvStorage.get(tested3.key));
    }

    @Test public void testClear() throws IOException {
        byte[] key = getRandomBytes(16);
        byte[] value = getRandomBytes(100);
        kvStorage.put(key, value);
        kvStorage.clear();
        assertNull(kvStorage.get(key));
    }

    @Test public void testInitNonEmptyBuffer() throws IOException {
        byte[] key1 = getRandomBytes(16);
        byte[] value1 = getRandomBytes(128);
        byte[] key2 = getRandomBytes(33);
        byte[] value2 = getRandomBytes(99);
        byte[] key3 = getRandomBytes(34);
        byte[] value3 = getRandomBytes(99);
        kvStorage.put(key1, value1);
        kvStorage.put(key2, value2);
        kvStorage = new KVStorageImpl(kvStorage.newBuffer);
        assertArrayEquals(value1, kvStorage.get(key1));
        assertArrayEquals(value2, kvStorage.get(key2));
        kvStorage.put(key3, value3);
        kvStorage = new KVStorageImpl(kvStorage.newBuffer);
        assertArrayEquals(value1, kvStorage.get(key1));
        assertArrayEquals(value2, kvStorage.get(key2));
        assertArrayEquals(value3, kvStorage.get(key3));
    }

    @Test public void testInitNonEmptyBuffer2() throws IOException {
        byte[] key1 = new byte[]{2, 3};
        byte[] value1 = new byte[]{20, 30};
        byte[] value2 = new byte[]{11, 12, 13};
        kvStorage.put(key1, value1);
        kvStorage = new KVStorageImpl(kvStorage.newBuffer);
        kvStorage.put(key1, value2);
        kvStorage = new KVStorageImpl(kvStorage.newBuffer);
        assertArrayEquals(value2, kvStorage.get(key1));
    }

    @Test public void testInitNonEmptyBuffer3() throws IOException {
        KVStorage.KeyValue[] keyValues = getRandomKV(12);
        KVStorage.KeyValue tested = keyValues[8];
        KVStorage.KeyValue tested2 = keyValues[0];
        KVStorage.KeyValue tested3 = keyValues[11];
        kvStorage.put(keyValues);
        kvStorage = new KVStorageImpl(kvStorage.newBuffer);
        assertArrayEquals(tested.value, kvStorage.get(tested.key));
        assertArrayEquals(tested2.value, kvStorage.get(tested2.key));
        keyValues = getRandomKV(5);
        keyValues[0] = new KVStorage.KeyValue(tested.key, getRandomBytes(14));
        keyValues[2] = new KVStorage.KeyValue(tested2.key, getRandomBytes(42));
        keyValues[4] = new KVStorage.KeyValue(tested3.key, getRandomBytes(46));
        kvStorage.put(keyValues);
        kvStorage = new KVStorageImpl(kvStorage.newBuffer);
        assertArrayEquals(keyValues[0].value, kvStorage.get(tested.key));
        assertArrayEquals(keyValues[2].value, kvStorage.get(tested2.key));
        assertArrayEquals(keyValues[4].value, kvStorage.get(tested3.key));
    }
}
