package io.k2iot.mcs.scheduler.trigger;

import io.k2iot.mcs.scheduler.command.SchedulerCommandException;
import io.k2iot.mcs.scheduler.job.JobDefinition;
import io.k2iot.mcs.scheduler.job.JobRepository;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public final class TriggerQueryService {

  private static final int DEFAULT_PAGE_SIZE = 100;
  private static final int MAX_PAGE_SIZE = 200;

  private final TriggerRepository triggerRepository;
  private final JobRepository jobRepository;

  public TriggerQueryService(TriggerRepository triggerRepository, JobRepository jobRepository) {
    this.triggerRepository = Objects.requireNonNull(triggerRepository, "triggerRepository");
    this.jobRepository = Objects.requireNonNull(jobRepository, "jobRepository");
  }

  public TriggerDefinition get(UUID triggerId, String namespace) {
    Objects.requireNonNull(triggerId, "triggerId");
    String requiredNamespace = requireNamespace(namespace);
    TriggerDefinition trigger =
        triggerRepository
            .findById(triggerId)
            .orElseThrow(
                () -> new SchedulerCommandException("TRIGGER_NOT_FOUND", "Trigger was not found"));
    if (!trigger.namespace().equals(requiredNamespace)) {
      throw new SchedulerCommandException("TRIGGER_NOT_FOUND", "Trigger was not found");
    }
    return trigger;
  }

  public Page list(String namespace, int requestedPageSize, String pageToken) {
    String requiredNamespace = requireNamespace(namespace);
    int pageSize = pageSize(requestedPageSize);
    Cursor cursor = decode(pageToken);
    List<TriggerDefinition> rows =
        triggerRepository.findPage(
            requiredNamespace, cursor.createdAt(), cursor.id(), pageSize + 1);
    return toPage(rows, pageSize);
  }

  public Page listByJob(
      UUID jobId, String namespace, int requestedPageSize, String pageToken) {
    Objects.requireNonNull(jobId, "jobId");
    String requiredNamespace = requireNamespace(namespace);
    JobDefinition job =
        jobRepository
            .findById(jobId)
            .orElseThrow(() -> new SchedulerCommandException("JOB_NOT_FOUND", "Job was not found"));
    if (!job.namespace().equals(requiredNamespace)) {
      throw new SchedulerCommandException("JOB_NOT_FOUND", "Job was not found");
    }
    int pageSize = pageSize(requestedPageSize);
    Cursor cursor = decode(pageToken);
    List<TriggerDefinition> rows =
        triggerRepository.findByJobIdPage(jobId, cursor.createdAt(), cursor.id(), pageSize + 1);
    return toPage(rows, pageSize);
  }

  private static Page toPage(List<TriggerDefinition> rows, int pageSize) {
    boolean hasMore = rows.size() > pageSize;
    List<TriggerDefinition> items =
        hasMore ? List.copyOf(rows.subList(0, pageSize)) : List.copyOf(rows);
    String nextPageToken =
        hasMore && !items.isEmpty()
            ? encode(
                items.get(items.size() - 1).createdAt(),
                items.get(items.size() - 1).triggerId())
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

  private static String encode(Instant createdAt, UUID id) {
    String value = createdAt + "|" + id;
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(value.getBytes(StandardCharsets.UTF_8));
  }

  private static Cursor decode(String token) {
    if (token == null || token.isBlank()) {
      return new Cursor(null, null);
    }
    try {
      String decoded =
          new String(Base64.getUrlDecoder().decode(token), StandardCharsets.UTF_8);
      String[] parts = decoded.split("\\|", -1);
      if (parts.length != 2) {
        throw new IllegalArgumentException("pageToken is invalid");
      }
      return new Cursor(Instant.parse(parts[0]), UUID.fromString(parts[1]));
    } catch (IllegalArgumentException exception) {
      throw new IllegalArgumentException("pageToken is invalid", exception);
    }
  }

  private record Cursor(Instant createdAt, UUID id) {}

  public record Page(List<TriggerDefinition> items, String nextPageToken) {
    public Page {
      items = List.copyOf(items);
    }
  }
}
