package kvstorage;

import java.io.*;

public class KVFileStorage extends KVByteStorage {
    private final File file;
    private final StreamWrapper streamWrapper;

    public KVFileStorage(File file) {
        this(file, new StreamWrapper() {
            @Override public InputStream inputStream(InputStream is) throws IOException {
                return is;
            }

            @Override public OutputStream outputStream(OutputStream os) throws IOException {
                return os;
            }
        });
    }

    public KVFileStorage(File file, StreamWrapper streamWrapper) {
        this.file = file;
        this.streamWrapper = streamWrapper;
    }

    @Override protected byte[] readBuffer() throws IOException {
        File backupFile = backupFile();
        byte[] result = null;
        try {
            if (backupFile.exists()) {
                deleteIfExists(file);
                if (!backupFile.renameTo(file)) {
                    throw new IOException("can't restore from backup");
                }
            }
            if (file.exists()) {
                InputStream stream = streamWrapper.inputStream(new FileInputStream(file));
                try {
                    int size = stream.available();
                    byte[] temp = new byte[size];
                    int read = stream.read(temp);
                    if (size != read) {
                        throw new IOException("can't read file " + size + " " + read);
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
                deleteIfExists(file);
                deleteIfExists(backupFile);
            }
        }
    }

    @Override protected void writeBuffer(byte[] newBuffer) throws IOException {
        File backupFile = backupFile();
        if (!backupFile.exists()) {////else use current backup file
            if (file.exists() && !file.renameTo(backupFile)) {
                throw new IOException("can't create backup");
            }
        }
        if (!file.exists() && !file.createNewFile()) {
            throw new IOException("can't create new file");
        }
        try {
            writeBuffer(newBuffer, file);
            deleteIfExists(backupFile);
        } catch (IOException ex) {
            deleteIfExists(file);//restore from backup next time
            throw ex;
        }
    }

    private void writeBuffer(byte[] buffer, File out) throws IOException {
        FileOutputStream fos = new FileOutputStream(out);
        OutputStream stream = streamWrapper.outputStream(fos);
        try {
            stream.write(buffer);
            stream.flush();
        } catch (Exception ex) {
            closeQuietly(stream);
            throw ex;
        }
        try {
            fos.getFD().sync();
        } catch (Exception ignored) {
        } finally {
            closeQuietly(stream);
        }
    }

    private File backupFile() {
        File parentDir = file.getParentFile();
        String backupName = file.getName() + ".b";
        if (parentDir != null) {
            return new File(parentDir, backupName);
        } else {
            return new File(backupName);
        }
    }

    private static boolean deleteIfExists(File file) {
        return !file.exists() || file.delete();
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
