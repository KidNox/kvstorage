package kvstorage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import static kvstorage.ByteUtils.*;

public final class KVByteStorage implements KVStorage {
    private final List<ByteEntry> entries = new ArrayList<>();

    private final ByteStorage byteStorage;
    private byte[] buffer;

    public KVByteStorage(ByteStorage byteStorage) throws IOException {
        this.byteStorage = byteStorage;
        byte[] buffer = byteStorage.read();
        readEntries(buffer);
        this.buffer = buffer;
    }

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
            entries.add(entry = ByteEntry.read(offset, buffer));
            if (entry.keyLength <= 0)
                throw new IOException("corrupted buffer " + offset + " " + size + " keyLength " + entry.keyLength);
            if (entry.valueLength < 0)
                throw new IOException("corrupted buffer " + offset + " " + size + " valueLength " + entry.valueLength);
            offset = entry.nextPosition();
        }
    }

    @Override public byte[] get(byte[] key) {
        long hash = hash(key);
        synchronized (this) {
            ByteEntry entry = find(key, hash, buffer);
            if (entry == null) return null;
            return entry.getOrReadValue(buffer);
        }
    }

    @Override public synchronized byte[] snapshot() {
        return subArray(buffer, 0, buffer.length);
    }

    @Override public void put(KeyValue... keyValues) throws IOException {
        if (keyValues.length == 0) return;
        byte[] currentBuffer;
        synchronized (this) {
            currentBuffer = buffer;
            for (KeyValue kv : keyValues) {
                currentBuffer = writeToBuffer(kv.key, kv.value, currentBuffer);
            }
        }
        writeNewBufferInternal(currentBuffer);
    }

    @Override public void put(byte[] key, byte[] value) throws IOException {
        byte[] newBuff = writeToBuffer(key, value, buffer);
        writeNewBufferInternal(newBuff);
    }

    @Override public boolean remove(byte[] key) throws IOException {
        byte[] newBuff;
        synchronized (this) {
            newBuff = writeToBuffer(key, null, buffer);
        }
        if (buffer != newBuff) {
            writeNewBufferInternal(newBuff);
            return true;
        }
        return false;
    }

    @Override public void clear() throws IOException {
        synchronized (this) {
            entries.clear();
        }
        writeNewBufferInternal(new byte[0]);
    }

    private byte[] writeToBuffer(byte[] key, byte[] value, byte[] buffer) {
        if (key.length == 0) throw new IllegalArgumentException("zero key length");
        if (key.length > ByteEntry.KEY_MAX_LENGTH)
            throw new IllegalArgumentException("key length > " + ByteEntry.KEY_MAX_LENGTH);
        long hash = hash(key);
        ByteEntry current = find(key, hash, buffer);
        if (current != null) {
            if (value == null) {
                return removeEntry(current, buffer);
            } else {
                return replaceEntry(current, value, buffer);
            }
        } else {
            if (value == null) return buffer;
            return addEntry(key, hash, value, buffer);
        }
    }

    private void writeNewBufferInternal(byte[] newBuffer) throws IOException {
        byte[] current = buffer;
        try {
            byteStorage.write(newBuffer);
            buffer = newBuffer;
        } catch (Exception ex) {
            synchronized (this) {
                readEntries(current);
            }
            throw ex;
        }
    }

    private ByteEntry find(byte[] key, long hash, byte[] buffer) {
        for (int i = 0; i < entries.size(); i++) {
            ByteEntry entry = entries.get(i);
            if (hash == entry.hash) {
                byte[] entryKey = entry.getOrReadKey(buffer);
                if (ByteUtils.equals(key, entryKey)) {
                    entry.setIndex(i);
                    return entry;
                }
            }
        }
        return null;
    }

    private byte[] addEntry(byte[] key, long hash, byte[] value, byte[] buffer) {
        ByteEntry byteEntry = new ByteEntry(buffer.length, hash, key.length, value.length);
        byte[] newBuff = new byte[byteEntry.nextPosition()];
        fill(buffer, newBuff, 0);
        int keyPos = byteEntry.write(byteEntry.position, newBuff);
        fill(key, newBuff, keyPos);
        fill(value, newBuff, byteEntry.valuePosition());
        entries.add(byteEntry);
        return newBuff;
    }

    private byte[] replaceEntry(ByteEntry current, byte[] value, byte[] buffer) {
        byte[] newBuff;
        if (current.valueLength == value.length) {
            newBuff = new byte[buffer.length];
            copy(buffer, 0, newBuff);
            fill(value, newBuff, current.valuePosition());
            current.setValue(value);
        } else {
            newBuff = new byte[buffer.length - current.valueLength + value.length];
            copy(buffer, 0, newBuff, 0, current.valuePosition());
            ByteEntry newEntry = current.copyWithNewValue(value);
            newEntry.write(current.position, newBuff);
            fill(value, newBuff, current.valuePosition());
            if (current.getIndex() < entries.size() - 1) {
                int destPos = current.valuePosition() + value.length;
                copy(buffer, current.nextPosition(), newBuff, destPos, newBuff.length - destPos);
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
        return newBuff;
    }

    private byte[] removeEntry(ByteEntry current, byte[] buffer) {
        byte[] newBuff = new byte[buffer.length - current.entryLength()];
        if (current.position > 0) {
            copy(buffer, 0, newBuff, 0, current.position);
        }
        if (current.nextPosition() < buffer.length) {
            copy(buffer, current.nextPosition(), newBuff, current.position, buffer.length - current.nextPosition());
        }
        entries.remove(current.getIndex());
        if (entries.size() > current.getIndex()) {
            ListIterator<ByteEntry> iterator = entries.listIterator(current.getIndex());
            ByteEntry previous = null;
            while (iterator.hasNext()) {
                ByteEntry next = iterator.next();
                if (previous == null) {
                    iterator.set(previous = next.copyWithNewPosition(current.position));
                } else {
                    iterator.set(previous = next.copyWithNewPosition(previous.nextPosition()));
                }
            }
        }
        return newBuff;
    }
}
