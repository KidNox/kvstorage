package kvstorage;

import java.io.*;

public class ByteFileStorage implements ByteStorage {
    private final File file;
    private final StreamWrapper streamWrapper;
    private final boolean strictWrite;

    protected ByteFileStorage(File file) {
        this(file, false, null);
    }

    protected ByteFileStorage(File file, boolean strictWrite, StreamWrapper streamWrapper) {
        this.file = file;
        this.strictWrite = strictWrite;
        this.streamWrapper = streamWrapper != null ? streamWrapper : new StreamWrapper() {
            @Override public InputStream input(InputStream is) throws IOException {
                return is;
            }

            @Override public OutputStream output(OutputStream os) throws IOException {
                return os;
            }
        };
    }

    protected KVStorage createStorage() throws IOException {
        return new KVByteStorage(this);
    }

    @Override public byte[] read() throws IOException {
        byte[] result = null;
        try {
            if (file.exists()) {
                InputStream stream = streamWrapper.input(new FileInputStream(file));
                try {
                    int size = stream.available();
                    byte[] temp = new byte[size];
                    int read = stream.read(temp);
                    if (size != read) {
                        throw new IOException("readBuffer failed " + size + " " + read);
                    }
                    result = temp;
                } finally {
                    closeQuietly(stream);
                }
            } else {
                result = new byte[0];
            }
            return result;
        } finally {
            if (result == null) {
                file.delete();
            }
        }
    }

    @Override public synchronized void write(byte[] bytes) throws IOException {
        File tmpFile = tempFile();
        boolean success;
        try {
            writeBuffer(bytes, tmpFile);
            success = tmpFile.renameTo(file);
        } catch (IOException ex) {
            throw new IOException("writeBuffer " + bytes.length, ex);
        } finally {
            tmpFile.delete();
        }
        if (!success) {
            throw new IOException("writeBuffer failed" + bytes.length);
        }
    }

    private void writeBuffer(byte[] buffer, File out) throws IOException {
        FileOutputStream fos = new FileOutputStream(out);
        OutputStream stream = streamWrapper.output(fos);
        try {
            stream.write(buffer);
            stream.flush();
        } catch (Exception ex) {
            closeQuietly(stream);
            throw ex;
        }
        if (strictWrite) {
            try {
                fos.getFD().sync();
            } catch (Exception ignored) {
            } finally {
                closeQuietly(stream);
            }
        }
    }

    protected File tempFile() throws IOException {
        return File.createTempFile(file.getName(), ".t", file.getParentFile());
    }

    private static void closeQuietly(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException ignored) {
            }
        }
    }
}
