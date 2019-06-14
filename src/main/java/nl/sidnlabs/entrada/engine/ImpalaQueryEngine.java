package nl.sidnlabs.entrada.engine;

import org.springframework.stereotype.Component;

@Component("athena")
public class ImpalaQueryEngine implements QueryEngine {

  @Override
  public boolean executeSql(String sql) {
    // TODO Auto-generated method stub
    return false;
  }

}
