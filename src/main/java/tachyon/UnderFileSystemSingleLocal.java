package tachyon;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import CodeTracer.CT;

/**
 * Single node UnderFilesystem implementation.
 * 
 * This only works for single machine. It is for local unit test and single machine mode.
 */
public class UnderFileSystemSingleLocal extends UnderFileSystem {

  public static UnderFileSystem getClient() {
    try (CT _ = new CT()) {
    return new UnderFileSystemSingleLocal();
  } }

  @Override
  public void close() throws IOException {
  }

  @Override
  public OutputStream create(String path) throws IOException {
    try (CT _ = new CT(path)) {
    //_.StackTrace();
    return new FileOutputStream(path);
  } }

  @Override
  public boolean delete(String path, boolean recursive) throws IOException {
    try (CT _ = new CT(path, recursive)) {
    File file = new File(path);
    return file.delete();
  } }

  @Override
  public boolean exists(String path) throws IOException {
    File file = new File(path);
    return file.exists();
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
    List<String> ret = new ArrayList<String>();
    ret.add(InetAddress.getLocalHost().getCanonicalHostName());
    return ret;
  }

  @Override
  public long getFileSize(String path) throws IOException {
    File file = new File(path);
    return file.length();
  }

  @Override
  public boolean mkdirs(String path, boolean createParent) throws IOException {
    File file = new File(path);
    if (createParent) {
      return file.mkdirs();
    }
    return file.mkdir();
  }

  @Override
  public InputStream open(String path) throws IOException {
    return new FileInputStream(path);
  }

  @Override
  public boolean rename(String src, String dst) throws IOException {
    File file = new File(src);
    return file.renameTo(new File(dst));
  }
}
