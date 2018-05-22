package kvstorage;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class KVStorageAdapterTest {
    private KVStorageAdapter adapter;

    @Before public void setUp() {
        adapter = new KVStorageAdapter(new KVStorageImpl());
    }

    @Test public void testBoolean() {
        adapter.put("key1", true);
        assertTrue(adapter.getBoolean("key1"));
        adapter.put("key1", false);
        assertFalse(adapter.getBoolean("key1"));
        assertTrue(adapter.getBoolean("key2", true));
    }

    @Test public void testInt() {
        adapter.putInt("key1", Integer.MIN_VALUE);
        assertEquals(Integer.MIN_VALUE, adapter.getInt("key1"));
        adapter.putInt("key1", Integer.MAX_VALUE);
        assertEquals(Integer.MAX_VALUE, adapter.getInt("key1"));
        adapter.putInt("key1", 0);
        assertEquals(0, adapter.getInt("key1"));
        assertEquals(0, adapter.getInt("key2"));
        adapter.putInt("key2", 42);
        assertEquals(42, adapter.getInt("key2"));
    }

    @Test public void testLong() {
        adapter.putLong("key1", Long.MIN_VALUE);
        assertEquals(Long.MIN_VALUE, adapter.getLong("key1"));
        adapter.putLong("key1", Long.MAX_VALUE);
        assertEquals(Long.MAX_VALUE, adapter.getLong("key1"));
        adapter.putLong("key1", 0);
        assertEquals(0, adapter.getLong("key1"));
        assertEquals(0, adapter.getLong("key2"));
        adapter.putLong("key2", -42);
        assertEquals(-42L, adapter.getLong("key2"));
    }

    @Test public void testDouble() {
        adapter.putDouble("key1", Double.MIN_VALUE);
        assertEquals(Double.MIN_VALUE, adapter.getDouble("key1"), 0);
        adapter.putDouble("key1", Double.MAX_VALUE);
        assertEquals(Double.MAX_VALUE, adapter.getDouble("key1"), 0);
        adapter.putDouble("key1", 0);
        assertEquals(0, adapter.getDouble("key1"), 0);
        assertEquals(0, adapter.getDouble("key2"), 0);
        adapter.putDouble("key2", -42);
        assertEquals(-42L, adapter.getDouble("key2"), 0);
    }

    @Test public void testString() {
        adapter.put("key1", "value1  ");
        assertEquals("value1  ", adapter.getString("key1"));
    }

    @Test public void testBytes() {
        byte[] value = Utils.getRandomBytes(13);
        adapter.put("key1", value);
        assertArrayEquals(value, adapter.getBytes("key1"));
    }

    @Test public void testBytes2() {
        byte[] key = Utils.getRandomBytes(9);
        byte[] value = Utils.getRandomBytes(13);
        adapter.put(key, value);
        assertArrayEquals(value, adapter.getBytes(key));
    }

    @Test public void testRemove() {
        adapter.put("key1", "value1");
        adapter.remove("key1");
        assertNull(adapter.getString("key1"));
    }

    @Test public void testRemoveEmpty() {
        adapter.remove("key1");
    }

    @Test public void testBulkInsert() {
        KVStorageAdapter.Editor editor = adapter.bulkInsert();
        editor.put("key1", true);
        assertFalse(adapter.getBoolean("key1"));
        editor.putInt("key2", 42);
        assertNotEquals(42, adapter.getInt("key2"));
        editor.put("key3", "value3");
        assertTrue(editor.commit());
        assertTrue(adapter.getBoolean("key1"));
        assertEquals(42, adapter.getInt("key2"));
        assertEquals("value3", adapter.getString("key3"));
    }

    @Test public void testBulkInsertAndRemove() {
        adapter.bulkInsert()
                .remove("null")
                .put("key1", true)
                .putInt("key2", 42)
                .put("key3", "value3")
                .commit();
        assertTrue(adapter.getBoolean("key1"));
        assertEquals(42, adapter.getInt("key2"));
        assertEquals("value3", adapter.getString("key3"));

        adapter.bulkInsert()
                .remove("key1")
                .remove("key2")
                .put("key3", "value4")
                .commit();
        assertFalse(adapter.getBoolean("key1"));
        assertEquals(0, adapter.getInt("key2"));
        assertEquals("value4", adapter.getString("key3"));
    }
}
