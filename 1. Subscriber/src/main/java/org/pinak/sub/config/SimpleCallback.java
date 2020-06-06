package org.pinak.sub.config;

import static org.pinak.sub.config.MqttMessageHandler.runMessageArrived;
import static org.pinak.sub.config.MqttMessageHandler.syncMessageArrived;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.protobuf.InvalidProtocolBufferException;

import lombok.extern.slf4j.Slf4j;

/**
 * This is the MQTT Callback class which overrides the MQTT Call back methods
 *
 */
@Slf4j
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
public class SimpleCallback implements MqttCallbackExtended {

	// Called when the client lost the connection to the broker
	@Override
	public void connectionLost(Throwable arg0) {
		log.error("Connection Lost: ", arg0);
	}

	// Called when a new message has arrived
	@Override
	public void messageArrived(String topic, MqttMessage message) throws InvalidProtocolBufferException {
		log.info("\nReceived a Message! \n\tTopic: " + topic + "\n");
		if (topic.startsWith("run")) {
			runMessageArrived(topic, message);
		} else {
			syncMessageArrived(topic, message);
		}
	}

	// This callback is invoked when a message published by this client
	// is successfully received by the broker.
	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {
		// logger.info("Delivery is Complete");
	}

	@Override
	public void connectComplete(boolean reconnect, String serverURI) {
		// Make or re-make subscriptions here
		if (reconnect) {
			log.info("Automatically Reconnected to Broker: {}", serverURI);

		} else {
			log.info("Connected for the first time To Broker: {}", serverURI);
		}
	}

}