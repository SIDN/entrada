package nl.sidnlabs.entrada.initialize;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.engine.QueryEngine;
import nl.sidnlabs.entrada.exception.ApplicationException;
import nl.sidnlabs.entrada.file.FileManager;
import nl.sidnlabs.entrada.file.FileManagerFactory;
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

  @Value("${entrada.location.work}")
  protected String work;

  @Value("${entrada.location.input}")
  protected String input;

  @Value("${entrada.location.output}")
  protected String output;

  @Value("${entrada.location.archive}")
  protected String archive;

  @Value("${entrada.icmp.enable}")
  protected boolean icmpEnabled;

  @Value("${aws.encryption}")
  protected boolean encrypt;


  private QueryEngine queryEngine;
  private String scriptPrefix;
  private FileManagerFactory fileManagerFactory;

  public AbstractInitializer(QueryEngine queryEngine, String scriptPrefix,
      FileManagerFactory fileManagerFactory) {
    this.queryEngine = queryEngine;
    this.scriptPrefix = scriptPrefix;
    this.fileManagerFactory = fileManagerFactory;
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

  public boolean initializeStorage() {
    log.info("Provision local storage");

    FileManager fileManager = fileManagerFactory.getFor(work);
    if (fileManager.isLocal() && fileManager.supported(work) && !fileManager.mkdir(work)) {
      throw new ApplicationException("Cannot create work location: " + work);
    }

    fileManager = fileManagerFactory.getFor(input);
    if (fileManager.isLocal() && fileManager.supported(input) && !fileManager.mkdir(input)) {
      throw new ApplicationException("Cannot create input location: " + input);
    }

    fileManager = fileManagerFactory.getFor(output);
    if (fileManager.isLocal() && fileManager.supported(output) && !fileManager.mkdir(output)) {
      throw new ApplicationException("Cannot create output location: " + output);
    }

    fileManager = fileManagerFactory.getFor(archive);
    if (fileManager.isLocal() && fileManager.supported(archive) && !fileManager.mkdir(archive)) {
      throw new ApplicationException("Cannot create output location: " + archive);
    }


    return true;
  }


  @Override
  public boolean initializeDatabase() {
    if (log.isDebugEnabled()) {
      log.debug("Provision database schema");
    }
    // create database
    Map<String, Object> parameters = dbParameters();
    String sql = TemplateUtil.template(sqlResource("create-database.sql"), parameters);
    queryEngine.execute(sql);

    // create dns table
    parameters = dnsParameters();
    sql = TemplateUtil.template(sqlResource("create-table-dns.sql"), parameters);
    queryEngine.execute(sql);

    // create icmp table
    if (icmpEnabled) {
      // create dns table
      parameters = icmpParameters();
      sql = TemplateUtil.template(sqlResource("create-table-icmp.sql"), parameters);
      queryEngine.execute(sql);
    }

    return true;
  }

  private ClassPathResource sqlResource(String script) {
    return new ClassPathResource("/sql/" + scriptPrefix + "/" + script, getClass());
  }


  private Map<String, Object> dbParameters() {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("DATABASE_NAME", database);
    parameters.put("DB_LOC", output);
    return parameters;
  }


  private Map<String, Object> dnsParameters() {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("DATABASE_NAME", database);
    parameters.put("TABLE_NAME", tableDns);
    parameters.put("TABLE_LOC", FileUtil.appendPath(output, tableDns));
    parameters.put("ENCRYPTED", encrypt);
    return parameters;
  }


  private Map<String, Object> icmpParameters() {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put("DATABASE_NAME", database);
    parameters.put("TABLE_NAME", tableIcmp);
    parameters.put("TABLE_LOC", FileUtil.appendPath(output, tableIcmp));
    parameters.put("ENCRYPTED", encrypt);
    return parameters;
  }

}
