package nl.sidnlabs.entrada.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@Configuration
public class ThreadConfig {

  @Value("${entrada.writer.thread.count:1}")
  private int writerThreadCount;

  @Value("${entrada.reader.thread.count:1}")
  private int readerThreadCount;

  /**
   * Create a custom threadpool for the taks that run using the @Scheduled annotation. by default
   * all tasks run using a single thread.
   * 
   * @return ThreadPoolTaskScheduler to be used by Spring for @Scheduled tasks
   */
  @Bean(destroyMethod = "shutdown")
  public ThreadPoolTaskScheduler taskExecutor() {
    ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
    taskScheduler.setPoolSize(5);
    taskScheduler.initialize();
    return taskScheduler;
  }

}
