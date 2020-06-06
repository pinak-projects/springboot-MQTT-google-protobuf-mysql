package org.pinak.sub.config;

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
public final class PublishResponse {

	private PublishResponse() {
	}

	public static boolean mqttPublishMessage(MqttAsyncClient mqttClient, byte[] response, String topic) {
		try {
			MqttMessage message = new MqttMessage(response);
			message.setQos(0);
			message.setRetained(false);
			mqttClient.publish(topic, message);
			return true;
		} catch (MqttException e) {
			log.error("Exception At: {}", e);
			return false;
		}
	}
}
