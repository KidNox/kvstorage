package kvstorage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import static kvstorage.ByteEntry.*;

public abstract class KVStorage {
    private final List<ByteEntry> entries = new ArrayList<>();

    private byte[] buffer;

    public void ensureBufferInitialized() throws IOException {
        if (buffer == null) {
            byte[] buffer = readBuffer();
            readEntries(buffer);
            this.buffer = buffer;
        }
    }

    protected abstract byte[] readBuffer() throws IOException;

    private void readEntries(byte[] buffer) throws IOException {
        entries.clear();
        int size = buffer.length;
        int offset = 0;
        ByteEntry entry;
        while (offset < size) {
            int remainingSize = size - offset;
            if (remainingSize < ByteEntry.LENGTH) {
                throw new IOException("corrupted buffer " + offset + " " + size + " " + entries.size());
            }
            entries.add(entry = read(offset, buffer));
            offset = entry.nextPosition();
        }
    }

    public void put(KeyValue... keyValues) throws IOException {
        if (keyValues.length == 0) return;
        ensureBufferInitialized();
        byte[] currentBuffer = buffer;
        for (KeyValue kv : keyValues) {
            currentBuffer = writeToBuffer(kv.key, kv.value, currentBuffer);
        }
        writeNewBuffer(currentBuffer);
    }

    public void put(byte[] key, byte[] value) throws IOException {
        ensureBufferInitialized();
        byte[] tempBuffer = writeToBuffer(key, value, buffer);
        writeNewBuffer(tempBuffer);
    }

    private byte[] writeToBuffer(byte[] key, byte[] value, byte[] buffer) {
        if (key.length == 0) throw new IllegalArgumentException("zero key length");
        if (key.length > Short.MAX_VALUE) throw new IllegalArgumentException("key length > 32767");
        long hash = hash(key);
        ByteEntry current = find(key, hash, buffer);
        if (current != null) {
            return replaceEntry(current, value, buffer);
        } else {
            return addEntry(key, hash, value, buffer);
        }
    }

    public byte[] get(byte[] key) throws IOException {
        ensureBufferInitialized();
        long hash = hash(key);
        ByteEntry entry = find(key, hash, buffer);
        if (entry == null) return null;
        return entry.getOrReadValue(buffer);
    }

    public void clear() throws IOException {
        entries.clear();
        writeNewBuffer(new byte[0]);
    }

    private ByteEntry find(byte[] key, long hash, byte[] buffer) {
        for (int i = 0; i < entries.size(); i++) {
            ByteEntry entry = entries.get(i);
            if (hash == entry.hash) {
                byte[] entryKey = entry.getOrReadKey(buffer);
                if (ByteEntry.equals(key, entryKey)) {
                    entry.setIndex(i);
                    return entry;
                }
            }
        }
        return null;
    }

    private byte[] addEntry(byte[] key, long hash, byte[] value, byte[] buffer) {
        ByteEntry byteEntry = new ByteEntry(buffer.length, hash, key.length, value.length);
        byte[] tempBuffer = new byte[byteEntry.nextPosition()];
        fill(buffer, tempBuffer, 0);
        int keyPos = byteEntry.write(byteEntry.position, tempBuffer);
        fill(key, tempBuffer, keyPos);
        fill(value, tempBuffer, byteEntry.valuePosition());
        entries.add(byteEntry);
        return tempBuffer;
    }

    private byte[] replaceEntry(ByteEntry current, byte[] value, byte[] buffer) {
        byte[] tempBuffer;
        if (current.valueLength == value.length) {
            tempBuffer = new byte[buffer.length];
            copy(buffer, 0, tempBuffer);
            fill(value, tempBuffer, current.valuePosition());
            current.setValue(value);
        } else {
            tempBuffer = new byte[buffer.length - current.valueLength + value.length];
            copy(buffer, 0, tempBuffer, 0, current.valuePosition());
            ByteEntry newEntry = current.copyWithNewValue(value);
            newEntry.write(current.position, tempBuffer);
            fill(value, tempBuffer, current.valuePosition());
            if (current.getIndex() < entries.size() - 1) {
                int destPos = current.valuePosition() + value.length;
                copy(buffer, current.nextPosition(), tempBuffer, destPos, tempBuffer.length - destPos);
                ListIterator<ByteEntry> iterator = entries.listIterator(current.getIndex());
                ByteEntry previous = current;
                while (iterator.hasNext()) {
                    ByteEntry next = iterator.next();
                    if (next == current) {
                        iterator.set(previous = newEntry);
                    } else {
                        iterator.set(previous = next.copyWithNewPosition(previous.nextPosition()));
                    }
                }
            } else {
                entries.set(current.getIndex(), newEntry);
            }
        }
        return tempBuffer;
    }

    protected void writeNewBuffer(byte[] newBuffer) throws IOException {
        buffer = newBuffer;
    }

    public static final class KeyValue {
        final byte[] key;
        final byte[] value;

        public KeyValue(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }
    }
}
