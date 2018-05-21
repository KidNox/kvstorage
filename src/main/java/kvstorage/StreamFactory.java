package kvstorage;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface StreamFactory {

    InputStream inputStream(File file) throws IOException;

    OutputStream outputStream(File file) throws IOException;
}
