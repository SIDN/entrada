package nl.sidnlabs.entrada.api;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import lombok.extern.log4j.Log4j2;
import nl.sidnlabs.entrada.SharedContext;

@Log4j2
@RestController
@RequestMapping("/status")
public class StatusController {

  private SharedContext sharedContext;

  public StatusController(SharedContext sharedContext) {
    this.sharedContext = sharedContext;
  }

  @PutMapping(value = "/stop")
  @ResponseStatus(HttpStatus.OK)
  public void stop() {
    log.info("Stop execution");
    sharedContext.setEnabled(false);
  }


  @PutMapping(value = "/start")
  @ResponseStatus(HttpStatus.OK)
  public void start() {
    log.info("Start execution");
    sharedContext.setEnabled(true);

  }

  @GetMapping
  public ExecutionStatus status() {
    log.info("Get execution status");
    return sharedContext.getStatus();
  }

}
