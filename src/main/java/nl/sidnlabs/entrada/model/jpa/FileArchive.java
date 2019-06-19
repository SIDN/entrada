package nl.sidnlabs.entrada.model.jpa;

import java.util.Date;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
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
public class FileArchive {

  @Id
  @Column(name = "id")
  @GeneratedValue(strategy = GenerationType.IDENTITY)
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

  @Column(name = "path")
  private String path;

  @Column(name = "rows")
  private long rows;

}
