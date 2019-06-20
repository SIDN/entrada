package nl.sidnlabs.entrada.initialize;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.engine.QueryEngine;
import nl.sidnlabs.entrada.exception.ApplicationException;
import nl.sidnlabs.entrada.util.FileUtil;
import nl.sidnlabs.entrada.util.TemplateUtil;

@Log4j2
public abstract class AbstractInitializer implements Initializer {

  @Value("${entrada.database.name}")
  protected String database;

  @Value("${entrada.database.table.dns}")
  protected String tableDns;

  @Value("${entrada.database.table.icmp}")
  protected String tableIcmp;

  @Value("${entrada.location.output}")
  protected String outputLocation;

  @Value("${entrada.icmp.enable}")
  protected boolean icmpEnabled;

  private QueryEngine queryEngine;
  private String scriptPrefix;

  public AbstractInitializer(QueryEngine queryEngine, String scriptPrefix) {
    this.queryEngine = queryEngine;
    this.scriptPrefix = scriptPrefix;
  }

  @PostConstruct
  public void init() {
    log.info("Perform provisioning");

    if (!initializeStorage()) {
      throw new ApplicationException("Error while initializing storage");
    }

    if (!initializeDatabase()) {
      throw new ApplicationException("Error while initializing database/tables");
    }
  }



  @Override
  public boolean initializeDatabase() {
    // create database
    Map<String, Object> parameters = dbParameters();
    String sql = TemplateUtil
        .template(new ClassPathResource("/sql/" + scriptPrefix + "/create-database.sql",
            TemplateUtil.class.getClass()), parameters);
    executeSQL(sql);

    // create dns table
    parameters = dnsParameters();
    sql = TemplateUtil
        .template(new ClassPathResource("/sql/" + scriptPrefix + "/create-table-dns.sql",
            TemplateUtil.class.getClass()), parameters);
    executeSQL(sql);

    // create icmp table
    if (icmpEnabled) {
      // create dns table
      parameters = icmpParameters();
      sql = TemplateUtil
          .template(new ClassPathResource("/sql/" + scriptPrefix + "/create-table-icmp.sql",
              TemplateUtil.class.getClass()), parameters);
      executeSQL(sql);
    }

    return true;
  }


  Map<String, Object> dbParameters() {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("DATABASE_NAME", database);
    parameters.put("DB_LOC", outputLocation);
    return parameters;
  }


  Map<String, Object> dnsParameters() {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("DATABASE_NAME", database);
    parameters.put("TABLE_NAME", tableDns);
    parameters.put("TABLE_LOC", FileUtil.appendPath(outputLocation, tableDns));
    return parameters;
  }


  Map<String, Object> icmpParameters() {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("DATABASE_NAME", database);
    parameters.put("TABLE_NAME", tableIcmp);
    parameters.put("TABLE_LOC", FileUtil.appendPath(outputLocation, tableIcmp));
    return parameters;
  }

  protected void executeSQL(String sql) {
    try {
      if (queryEngine.execute(sql).get(5, TimeUnit.MINUTES).equals(Boolean.FALSE)) {
        // failed to execute sql
        throw new ApplicationException("Query failed");
      }
    } catch (Exception e) {
      throw new ApplicationException("Query failed");
    }
  }
}
