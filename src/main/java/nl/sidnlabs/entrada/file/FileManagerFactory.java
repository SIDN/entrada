package nl.sidnlabs.entrada.file;

public interface FileManagerFactory {

  FileManager getFor(String file);

  static FileManager local() {
    return new LocalFileManagerImpl();
  }
}
