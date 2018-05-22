package kvstorage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface StreamWrapper {

    InputStream inputStream(InputStream is) throws IOException;

    OutputStream outputStream(OutputStream os) throws IOException;
}
