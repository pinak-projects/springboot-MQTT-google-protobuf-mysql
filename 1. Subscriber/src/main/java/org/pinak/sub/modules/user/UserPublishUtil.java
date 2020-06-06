package org.pinak.sub.modules.user;

import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.pinak.sub.constants.Constants;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
public class UserPublishUtil {

	private UserPublishUtil() {
	}

	public static boolean mqttPublishUsers(MqttAsyncClient mqttClient, byte[] usersList, long clientId) {
		try {
			MqttMessage message = new MqttMessage(usersList);
			message.setQos(0);
			message.setRetained(false);
			mqttClient.publish("ams/sync/users/" + clientId, message);
			return true;
		} catch (MqttException e) {
			log.error(Constants.EXCEPTION, e);
			return false;
		}
	}
}
