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
        boolean success = false;
        try {
            if (backupFile.exists()) {
                deleteIfExists(file);
                if (!backupFile.renameTo(file)) {
                    throw new IOException("can't restore from backup");
                }
            }
            if (file.exists()) {
                InputStream stream = streamWrapper.inputStream(new FileInputStream(file));
                int size = stream.available();
                byte[] buffer = new byte[size];
                int read = stream.read(buffer);
                if (size != read) {
                    throw new IOException("can't read file " + size + " " + read);
                }
                stream.close();
                success = true;
                return buffer;
            } else {
                success = true;
                return new byte[0];
            }
        } finally {
            if (!success) {
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
        stream.write(buffer);
        stream.flush();
        try {
            fos.getFD().sync();
        } catch (Exception ignored) {
        } finally {
            stream.close();
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
}
