package org.pinak.sub.modules.run;

import org.pinak.sub.config.MqttUtil;
import org.pinak.sub.model.RunResponseProto.RunResponse;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
public final class RunService {

	private RunService() {
	}

	public static void generateRunId(long clientId) {

		RunPublishUtil.mqttPublishRunId(MqttUtil.mqttAsyncClient,
				RunResponse.newBuilder().setRunId("R11-001").build().toByteArray(), clientId);
	}
}
