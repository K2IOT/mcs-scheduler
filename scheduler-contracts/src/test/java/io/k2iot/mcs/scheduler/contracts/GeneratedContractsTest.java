package io.k2iot.mcs.scheduler.contracts;

import static org.assertj.core.api.Assertions.assertThat;

import io.k2iot.mcs.scheduler.v1.CronTrigger;
import io.k2iot.mcs.scheduler.v1.TriggerSpec;
import org.junit.jupiter.api.Test;

class GeneratedContractsTest {

  @Test
  void cronTriggerRoundTripsThroughProtobuf() throws Exception {
    TriggerSpec source =
        TriggerSpec.newBuilder()
            .setCron(
                CronTrigger.newBuilder()
                    .setExpression("0 0 8 * * ?")
                    .setTimezone("Asia/Ho_Chi_Minh"))
            .build();

    assertThat(TriggerSpec.parseFrom(source.toByteArray())).isEqualTo(source);
  }
}
