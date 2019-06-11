package nl.sidnlabs.entrada.file;

import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import org.springframework.stereotype.Component;
import com.amazonaws.services.s3.AmazonS3;

@Component
public class S3FileManagerImpl implements FileManager {

  private AmazonS3 amazonS3;

  public S3FileManagerImpl(AmazonS3 amazonS3) {
    this.amazonS3 = amazonS3;

    amazonS3.listBuckets().stream().forEach(System.out::print);
    System.out.println("");
  }

  @Override
  public String schema() {
    return "s3://";
  }

  @Override
  public boolean exists(String file) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public List<String> files(String dir, String... filter) {
    // amazonS3.listObjects("")
    return null;
  }

  @Override
  public Optional<InputStream> open(String filename) {
    // TODO Auto-generated method stub
    return null;
  }

}
