package nl.sidnlabs.entrada.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import org.springframework.core.io.ClassPathResource;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.exception.ApplicationException;

@Log4j2
public class TemplateUtil {

  private TemplateUtil() {}

  public static String template(ClassPathResource template, Map<String, Object> values) {

    String sql = StrSubstitutor.replace(readTemplate(template), values);

    if (log.isDebugEnabled()) {
      log.debug("Template result: {}", sql);
    }

    return sql;
  }

  public static String template(String template, Map<String, Object> values) {

    String sql = StrSubstitutor.replace(template, values);

    if (log.isDebugEnabled()) {
      log.debug("Template result: {}", sql);
    }

    return sql;
  }

  private static String readTemplate(ClassPathResource template) {
    try (InputStream is = template.getInputStream()) {
      return IOUtils.toString(template.getInputStream(), StandardCharsets.UTF_8.name());
    } catch (IOException e) {
      throw new ApplicationException("Could not load file " + template, e);
    }
  }

}
