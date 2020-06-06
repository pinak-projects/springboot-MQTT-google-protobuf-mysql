package org.pinak.sub.config;

import java.io.IOException;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

/**
 * This class holds the MQTT methods to Connect, Publish & Subscribe to Broker
 *
 */
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
public class MqttUtil {

	private static final Logger logger = LogManager.getLogger(MqttUtil.class);

	private static final String PROPERTIES_FILE_NAME = "/mqtt.properties";
	private static Properties props = new Properties();

	public static MqttAsyncClient mqttAsyncClient;
	
	public void mqttConnectAndSubscribe(String clientId) throws MqttException {
		final MemoryPersistence persistence = new MemoryPersistence();
		try {
			MqttUtil.props.load(MqttUtil.class.getResourceAsStream(PROPERTIES_FILE_NAME));
			MqttAsyncClient mqttClient = new MqttAsyncClient(props.getProperty("BROKER_URL"), clientId, persistence);
			MqttConnectOptions connOpts = new MqttConnectOptions();
			connOpts.setAutomaticReconnect(true);
			connOpts.setCleanSession(true);
			connOpts.setKeepAliveInterval(10);
			connOpts.setConnectionTimeout(0);
			connOpts.setUserName("subscriber");
			connOpts.setPassword("subscriber".toCharArray());
			System.out.println(connOpts.toString());
			String[] serverURIarray = new String[] { props.getProperty("BROKER_URL") };
			connOpts.setServerURIs(serverURIarray);
			logger.info("About to connect to MQTT broker with the following parameters: - BROKER_URL=" + props.getProperty("BROKER_URL") + " CLIENT_ID=" + clientId);
			mqttClient.setCallback(new SimpleCallback());
			mqttClient.connect(connOpts, null, new IMqttActionListener() {
				@Override
				public void onSuccess(IMqttToken conToken) {
					logger.info("Connected to: " + mqttClient.getCurrentServerURI() + ", CLIENT_ID: " + mqttClient.getClientId());
					mqttAsyncClient = mqttClient;
					subscribe(mqttClient);
				}
				@Override
				public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
					logger.info("Failed to connect to: " + mqttClient.getServerURI());
					logger.info("Message: " + exception.getMessage());
					logger.info("Cause: " + exception.getCause());
					exception.printStackTrace();
				}
			});
		} catch (MqttException | IOException e) {
			logger.error("msg: " + e.getMessage());
			logger.error("cause: " + e.getCause());
			e.printStackTrace();
			System.exit(-1);
		}
	}

	// Subscribe client to the topic with QoS level of 0
	private void subscribe(MqttAsyncClient mqttClient) {
		try {
			int subQoS = 0;
			String syncTopic = props.getProperty("SYNC_WILDCARD_TOPIC");
			mqttClient.subscribe(syncTopic, subQoS, null, new IMqttActionListener() {
				@Override
				public void onSuccess(IMqttToken asyncActionToken) {
					logger.info("Successfully Subscribed, client: {} to the topic: {}", mqttClient.getClientId(), syncTopic);
				}
				@Override
				public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
					logger.error("Error subscribing: {}", asyncActionToken.getException());
				}
			});
			String runTopic = props.getProperty("RUN_WILDCARD_TOPIC");
			mqttClient.subscribe(runTopic, subQoS, null, new IMqttActionListener() {
				@Override
				public void onSuccess(IMqttToken asyncActionToken) {
					logger.info("Successfully Subscribed, client: {} to the topic: {}", mqttClient.getClientId(), runTopic);
				}
				@Override
				public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
					logger.error("Error subscribing: {}", asyncActionToken.getException());
				}
			});
		} catch (MqttException ex) {
			logger.error("Exception whilst subscribing");
			ex.printStackTrace();
		}
	}

}