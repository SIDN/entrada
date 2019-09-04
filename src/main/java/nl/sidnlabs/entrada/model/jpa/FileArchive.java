package nl.sidnlabs.entrada.model.jpa;

import java.util.Date;
import javax.persistence.AttributeConverter;
import javax.persistence.Column;
import javax.persistence.Converter;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import org.apache.commons.lang3.StringUtils;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "entrada_file_archive")
@SequenceGenerator(name = "file_id_seq", sequenceName = "FILE_ID_SEQ", allocationSize = 1)
public class FileArchive {

  public enum ArchiveModeType {
    NONE, ARCHIVE, DELETE
  }

  @Id
  @Column(name = "id")
  @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "file_id_seq")
  private int id;

  @Column(name = "date_start")
  @Temporal(TemporalType.TIMESTAMP)
  private Date dateStart;

  @Column(name = "date_end")
  @Temporal(TemporalType.TIMESTAMP)
  private Date dateEnd;

  @Column(name = "time")
  private int time;

  @Column(name = "file")
  private String file;

  @Column(name = "server")
  private String server;

  @Column(name = "path")
  private String path;

  @Column(name = "rows")
  private long rows;

  @Column(name = "mode")
  private ArchiveModeType mode;


  @Converter(autoApply = true)
  public static class ArchiveModeTypeConverter
      implements AttributeConverter<ArchiveModeType, String> {

    @Override
    public String convertToDatabaseColumn(ArchiveModeType ct) {
      if (ct != null) {
        return ct.name();
      }

      return null;
    }

    @Override
    public ArchiveModeType convertToEntityAttribute(String dbData) {
      if (dbData != null) {
        return ArchiveModeType.valueOf(StringUtils.upperCase(dbData));
      }

      return null;
    }
  }

}
