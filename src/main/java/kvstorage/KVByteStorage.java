package kvstorage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import static kvstorage.ByteUtils.*;

public abstract class KVByteStorage implements KVStorage {
    private final List<ByteEntry> entries = new ArrayList<>();

    private byte[] buffer;

    void ensureBufferInitialized() throws IOException {
        if (buffer == null) {
            try {
                byte[] buf = readBuffer();
                readEntries(buf);
                buffer = buf;
            } finally {
                if (buffer == null) {
                    readEntries(buffer = new byte[0]);
                }
            }
        }
    }

    protected abstract byte[] readBuffer() throws IOException;

    protected abstract void writeBuffer(byte[] newBuffer) throws IOException;

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

    @Override public void loadToBuffer() throws IOException {
        ensureBufferInitialized();
    }

    @Override public void put(KeyValue... keyValues) throws IOException {
        if (keyValues.length == 0) return;
        ensureBufferInitialized();
        byte[] currentBuffer = buffer;
        for (KeyValue kv : keyValues) {
            currentBuffer = writeToBuffer(kv.key, kv.value, currentBuffer);
        }
        writeNewBufferInternal(currentBuffer);
    }

    @Override public void put(byte[] key, byte[] value) throws IOException {
        ensureBufferInitialized();
        byte[] tempBuffer = writeToBuffer(key, value, buffer);
        writeNewBufferInternal(tempBuffer);
    }

    @Override public boolean remove(byte[] key) throws IOException {
        ensureBufferInitialized();
        byte[] tempBuffer = writeToBuffer(key, null, buffer);
        if (buffer != tempBuffer) {
            writeNewBufferInternal(tempBuffer);
            return true;
        }
        return false;
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

    @Override public byte[] get(byte[] key) throws IOException {
        ensureBufferInitialized();
        long hash = hash(key);
        ByteEntry entry = find(key, hash, buffer);
        if (entry == null) return null;
        return entry.getOrReadValue(buffer);
    }

    @Override public void clear() throws IOException {
        entries.clear();
        writeNewBufferInternal(new byte[0]);
    }

    private void writeNewBufferInternal(byte[] newBuffer) throws IOException {
        try {
            writeBuffer(newBuffer);
            buffer = newBuffer;
        } finally {
            if (buffer != newBuffer) {
                readEntries(buffer);
            }
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

    private byte[] removeEntry(ByteEntry current, byte[] buffer) {
        byte[] tempBuffer = new byte[buffer.length - current.entryLength()];
        if (current.position > 0) {
            copy(buffer, 0, tempBuffer, 0, current.position);
        }
        if (current.nextPosition() < buffer.length) {
            copy(buffer, current.nextPosition(), tempBuffer, current.position, buffer.length - current.nextPosition());
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
        return tempBuffer;
    }
}
