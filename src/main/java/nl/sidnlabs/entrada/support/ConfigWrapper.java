package nl.sidnlabs.entrada.support;

import org.springframework.core.env.Environment;

public class ConfigWrapper {

  private Environment env;

  public ConfigWrapper(Environment env) {
    this.env = env;
  }

  public String asString(String key, String dv) {
    try {
      return env.getProperty(key, dv);
    } catch (Exception e) {
      return dv;
    }
  }

  public int asInt(String key, int dv) {
    try {
      return Integer.parseInt(env.getProperty(key));
    } catch (Exception e) {
      return dv;
    }
  }

  public boolean asBoolean(String key, boolean dv) {
    try {
      return Boolean.parseBoolean(env.getProperty(key));
    } catch (Exception e) {
      return dv;
    }
  }

}
