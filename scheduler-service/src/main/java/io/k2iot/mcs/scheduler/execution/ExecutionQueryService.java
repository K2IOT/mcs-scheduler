package io.k2iot.mcs.scheduler.execution;

import io.k2iot.mcs.scheduler.command.SchedulerCommandException;
import io.k2iot.mcs.scheduler.job.JobDefinition;
import io.k2iot.mcs.scheduler.job.JobRepository;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public final class ExecutionQueryService {

  private static final int DEFAULT_PAGE_SIZE = 100;
  private static final int MAX_PAGE_SIZE = 200;

  private final ExecutionRepository executionRepository;
  private final JobRepository jobRepository;

  public ExecutionQueryService(
      ExecutionRepository executionRepository, JobRepository jobRepository) {
    this.executionRepository = Objects.requireNonNull(executionRepository, "executionRepository");
    this.jobRepository = Objects.requireNonNull(jobRepository, "jobRepository");
  }

  public ExecutionRepository.ExecutionRecord get(UUID executionId, String namespace) {
    Objects.requireNonNull(executionId, "executionId");
    String requiredNamespace = requireNamespace(namespace);
    ExecutionRepository.ExecutionRecord execution =
        executionRepository
            .findById(executionId)
            .orElseThrow(
                () ->
                    new SchedulerCommandException(
                        "EXECUTION_NOT_FOUND", "Execution was not found"));
    JobDefinition job =
        jobRepository
            .findById(execution.jobId())
            .orElseThrow(
                () ->
                    new SchedulerCommandException(
                        "EXECUTION_NOT_FOUND", "Execution was not found"));
    if (!job.namespace().equals(requiredNamespace)) {
      throw new SchedulerCommandException("EXECUTION_NOT_FOUND", "Execution was not found");
    }
    return execution;
  }

  public Page list(String namespace, int requestedPageSize, String pageToken) {
    String requiredNamespace = requireNamespace(namespace);
    int pageSize = pageSize(requestedPageSize);
    Cursor cursor = decode(pageToken);
    List<ExecutionRepository.ExecutionRecord> rows =
        executionRepository.findPage(
            requiredNamespace, cursor.scheduledFireTime(), cursor.id(), pageSize + 1);
    boolean hasMore = rows.size() > pageSize;
    List<ExecutionRepository.ExecutionRecord> items =
        hasMore ? List.copyOf(rows.subList(0, pageSize)) : List.copyOf(rows);
    String nextPageToken =
        hasMore && !items.isEmpty()
            ? encode(
                items.get(items.size() - 1).scheduledFireTime(),
                items.get(items.size() - 1).executionId())
            : null;
    return new Page(items, nextPageToken);
  }

  private static int pageSize(int requested) {
    if (requested < 0) {
      throw new IllegalArgumentException("pageSize must not be negative");
    }
    if (requested == 0) {
      return DEFAULT_PAGE_SIZE;
    }
    return Math.min(requested, MAX_PAGE_SIZE);
  }

  private static String requireNamespace(String namespace) {
    if (namespace == null || namespace.isBlank()) {
      throw new IllegalArgumentException("namespace must not be blank");
    }
    return namespace;
  }

  private static String encode(Instant scheduledFireTime, UUID id) {
    String value = (scheduledFireTime == null ? "~" : scheduledFireTime.toString()) + "|" + id;
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(value.getBytes(StandardCharsets.UTF_8));
  }

  private static Cursor decode(String token) {
    if (token == null || token.isBlank()) {
      return new Cursor(null, null);
    }
    try {
      String decoded = new String(Base64.getUrlDecoder().decode(token), StandardCharsets.UTF_8);
      String[] parts = decoded.split("\\|", -1);
      if (parts.length != 2) {
        throw new IllegalArgumentException("pageToken is invalid");
      }
      Instant scheduledFireTime = "~".equals(parts[0]) ? null : Instant.parse(parts[0]);
      return new Cursor(scheduledFireTime, UUID.fromString(parts[1]));
    } catch (IllegalArgumentException exception) {
      throw new IllegalArgumentException("pageToken is invalid", exception);
    }
  }

  private record Cursor(Instant scheduledFireTime, UUID id) {}

  public record Page(List<ExecutionRepository.ExecutionRecord> items, String nextPageToken) {
    public Page {
      items = List.copyOf(items);
    }
  }
}
