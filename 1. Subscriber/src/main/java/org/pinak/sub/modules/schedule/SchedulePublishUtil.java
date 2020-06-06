package org.pinak.sub.modules.schedule;

import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
public class SchedulePublishUtil {

	private SchedulePublishUtil() {
	}

	public static boolean mqttPublishSchedules(MqttAsyncClient mqttClient, byte[] schedulesList, long clientId) {
		try {
			MqttMessage message = new MqttMessage(schedulesList);
			message.setQos(0);
			message.setRetained(false);
			mqttClient.publish("ams/sync/schedules/" + clientId, message);
			return true;
		} catch (MqttException e) {
			log.error("msg: " + e.getMessage());
			log.error("cause: " + e.getCause());
			e.printStackTrace();
			return false;
		}
	}
}
