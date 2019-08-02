package nl.sidnlabs.entrada.api;

import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class ExecutionStatus {
  private boolean enabled;
  private boolean execution;
  private boolean compaction;
  private boolean maintenance;
}
