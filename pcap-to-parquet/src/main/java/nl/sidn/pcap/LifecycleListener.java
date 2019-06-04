package nl.sidn.pcap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.context.Lifecycle;
import org.springframework.stereotype.Component;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Component
public class LifecycleListener implements Lifecycle {

  private boolean running = false;
  @Autowired
  private ApplicationArguments applicationArguments;

  @Override
  public void start() {
    if (log.isDebugEnabled()) {
      log.debug("Start()");
    }
    running = true;
  }

  @Override
  public void stop() {
    if (log.isDebugEnabled()) {
      log.debug("Stop()");
    }
    running = false;
  }

  @Override
  public boolean isRunning() {
    return running;
  }

}
