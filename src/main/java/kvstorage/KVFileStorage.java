package kvstorage;

import java.io.*;

public class KVFileStorage extends KVStorage {
    private final File file;
    private final StreamFactory streamFactory;

    public KVFileStorage(File file) {
        this(file, new StreamFactory() {
            @Override public InputStream inputStream(File file) throws IOException {
                return new FileInputStream(file);
            }

            @Override public OutputStream outputStream(File file) throws IOException {
                return new FileOutputStream(file);
            }
        });
    }

    public KVFileStorage(File file, StreamFactory streamFactory) {
        this.file = file;
        this.streamFactory = streamFactory;
    }

    @Override protected byte[] readBuffer() throws IOException {
        File backupFile = backupFile();
        if (backupFile.exists()) {
            deleteIfExists(file);
            if (!backupFile.renameTo(file)) {
                throw new IOException("can't restore from backup");
            }
        }
        if (file.exists()) {
            InputStream stream = streamFactory.inputStream(file);
            int size = stream.available();
            byte[] buffer = new byte[size];
            int read = stream.read(buffer);
            if (size != read) {
                throw new IOException("can't read file " + size + " " + read);
            }
            stream.close();
            return buffer;
        } else {
            return new byte[0];
        }
    }

    @Override protected void writeNewBuffer(byte[] newBuffer) throws IOException {
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
        super.writeNewBuffer(newBuffer);
    }

    private void writeBuffer(byte[] buffer, File out) throws IOException {
        OutputStream stream = streamFactory.outputStream(out);
        stream.write(buffer);
        stream.close();
    }

    private File backupFile() {
        return new File(file.getAbsolutePath() + ".b");
    }

    private static boolean deleteIfExists(File file) {
        return !file.exists() || file.delete();
    }
}
