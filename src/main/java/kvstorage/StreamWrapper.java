package kvstorage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface StreamWrapper {
    InputStream input(InputStream is) throws IOException;

    OutputStream output(OutputStream os) throws IOException;
}
