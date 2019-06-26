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
@Table(name = "entrada_partition")
public class TablePartition {

  @Id
  @Column(name = "id")
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private int id;

  @Column(name = "engine")
  private String engine;

  @Column(name = "table_name")
  private String table;

  @Column(name = "created")
  @Temporal(TemporalType.TIMESTAMP)
  private Date created;

  @Column(name = "year")
  private int year;

  @Column(name = "month")
  private int month;

  @Column(name = "day")
  private int day;

  @Column(name = "server")
  private String server;

  @Column(name = "path")
  private String path;

  @Column(name = "compaction_ts")
  @Temporal(TemporalType.TIMESTAMP)
  private Date compaction;

  @Column(name = "compaction_time")
  private int compactionTime;

  @Column(name = "updated_ts")
  @Temporal(TemporalType.TIMESTAMP)
  private Date updated;

  public String toPath() {
    return "year=" + year + "/month=" + month + "/day=" + day + "/server="
        + StringUtils.defaultIfBlank(server, "__default__");
  }

}
