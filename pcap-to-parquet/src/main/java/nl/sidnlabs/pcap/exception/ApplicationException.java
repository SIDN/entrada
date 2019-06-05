package nl.sidnlabs.pcap.exception;

public class ApplicationException extends RuntimeException {

  private static final long serialVersionUID = -256674460368957041L;


  public ApplicationException(String message, Throwable cause) {
    super(message, cause);
  }

  public ApplicationException(String message) {
    super(message);
  }

}
