package com.pinak.pub.mqtt;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import com.google.protobuf.StringValue;
import com.pinak.pub.model.AmsProcessProto.AmsProcess;
import com.pinak.pub.model.AmsSyncAckProto.AmsSyncAck;
import com.pinak.pub.model.DeviceConnect.DeviceConnectProto;
import com.pinak.pub.model.RunRequestProto.RunRequest;
import com.pinak.pub.model.SyncRequestProto.SyncRequest;

/**
 * This class holds the MQTT methods to Connect, Publish & Subscribe to Broker
 *
 */
public class MqttUtil {

	private static final Logger logger = LogManager.getLogger(MqttUtil.class);

	private static MqttAsyncClient asyncClient;

	private static final String PROPERTIES_FILE_NAME = "/mqtt.properties";
	Properties props = new Properties();

	public void mqttConnectAndSubscribe(String clientId) throws MqttException {
		final MemoryPersistence persistence = new MemoryPersistence();
		try {
			this.props.load(MqttUtil.class.getResourceAsStream(PROPERTIES_FILE_NAME));
			MqttAsyncClient mqttClient = new MqttAsyncClient(props.getProperty("BROKER_URL"), clientId, persistence);
			MqttConnectOptions connOpts = new MqttConnectOptions();
			connOpts.setAutomaticReconnect(true);
			connOpts.setCleanSession(true);
			connOpts.setKeepAliveInterval(30);
			connOpts.setUserName("publisher1");
			connOpts.setPassword("publisher1".toCharArray());
			String[] serverURIarray = new String[] { props.getProperty("BROKER_URL") };
			connOpts.setServerURIs(serverURIarray);
			logger.info("About to connect to MQTT broker with the following parameters: - BROKER_URL="
					+ props.getProperty("BROKER_URL") + " CLIENT_ID=" + clientId);
			mqttClient.setCallback(new SimpleCallback());
			mqttClient.connect(connOpts, null, new IMqttActionListener() {
				@Override
				public void onSuccess(IMqttToken conToken) {
					logger.info("Connected to: " + mqttClient.getCurrentServerURI() + ", CLIENT_ID: "
							+ mqttClient.getClientId());
					asyncClient = mqttClient;
					subscribe(mqttClient);
					// mqttConnected(mqttClient, time);
				}

				@Override
				public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
					logger.info("Failed to connect to: " + mqttClient.getServerURI());
				}
			});
			// mqttPublishMessageStream(mqttClient);
		} catch (MqttException | IOException e) {
			logger.error("msg: " + e.getMessage());
			logger.error("cause: " + e.getCause());
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public static void mqttPublish(MqttAsyncClient mqttClient) {
		try {
			SyncRequest userSyncRequest = SyncRequest.newBuilder().setClientId(Long.parseLong(mqttClient.getClientId()))
					.build();
			MqttMessage message = new MqttMessage(userSyncRequest.toByteArray());
			message.setQos(0);
			logger.info("sending users request...");
			mqttClient.publish("sync/users/request", message);
		} catch (MqttException e) {
			logger.error("msg: " + e.getMessage());
			logger.error("cause: " + e.getCause());
			e.printStackTrace();
			System.exit(-1);
		}
	}

	// Subscribe client to the topic with QoS level of 0
	public void subscribe(MqttAsyncClient mqttClient) {
		try {
			int subQoS = 0;
			String topic = props.getProperty("TOPIC_NAME");
			mqttClient.subscribe(topic, subQoS, null, new IMqttActionListener() {
				@Override
				public void onSuccess(IMqttToken asyncActionToken) {
					logger.info("Successfully Subscribed, client: {} to the topic: {}", mqttClient.getClientId(),
							topic);
					// mqttPublish(mqttClient);
				}

				@Override
				public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
					logger.error("Error subscribing: " + asyncActionToken.getException());
				}
			});
			String topic2 = "response/#";
			mqttClient.subscribe(topic2, subQoS, null, new IMqttActionListener() {
				@Override
				public void onSuccess(IMqttToken asyncActionToken) {
					logger.info("Successfully Subscribed, client: {} to the topic: {}", mqttClient.getClientId(),
							topic2);
				}

				@Override
				public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
					logger.error("Error subscribing: " + asyncActionToken.getException());
				}
			});
			String topic3 = "ams/sync/schedules/".concat(mqttClient.getClientId());
			mqttClient.subscribe(topic3, subQoS, null, new IMqttActionListener() {
				@Override
				public void onSuccess(IMqttToken asyncActionToken) {
					logger.info("Successfully Subscribed, client: {} to the topic: {}", mqttClient.getClientId(),
							topic3);
				}

				@Override
				public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
					logger.error("Error subscribing: " + asyncActionToken.getException());
				}
			});
			String topic4 = "ams/sync/process/".concat(mqttClient.getClientId());
			mqttClient.subscribe(topic4, subQoS, null, new IMqttActionListener() {
				@Override
				public void onSuccess(IMqttToken asyncActionToken) {
					logger.info("Successfully Subscribed, client: {} to the topic: {}", mqttClient.getClientId(),
							topic4);
				}

				@Override
				public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
					logger.error("Error subscribing: " + asyncActionToken.getException());
				}
			});
			String topic5 = "ams/run/response/".concat(mqttClient.getClientId());
			mqttClient.subscribe(topic5, subQoS, null, new IMqttActionListener() {
				@Override
				public void onSuccess(IMqttToken asyncActionToken) {
					logger.info("Successfully Subscribed, client: {} to the topic: {}", mqttClient.getClientId(),
							topic5);
					// mqttPublish(mqttClient);
					mqttRunRequest();
				}

				@Override
				public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
					logger.error("Error subscribing: " + asyncActionToken.getException());
				}
			});
		} catch (MqttException ex) {
			logger.error("Exception whilst subscribing");
			ex.printStackTrace();
		}
	}

	public void mqttConnected(MqttAsyncClient mqttClient, long time) {
		DeviceConnectProto deviceConnectProto = DeviceConnectProto.newBuilder().setClientId(mqttClient.getClientId())
				.setTime(time).build();
		MqttMessage message = new MqttMessage(deviceConnectProto.toByteArray());
		message.setQos(1);
		try {
			mqttClient.publish("iot/connect", message);
			logger.info("Message: " + new String(message.getPayload()));
		} catch (MqttException e) {
			logger.error("msg: " + e.getMessage());
			logger.error("cause: " + e.getCause());
			e.printStackTrace();
			System.exit(-1);
		}
	}

	private static List<String> param1Points = new ArrayList<>();
	private static List<String> param2Points = new ArrayList<>();

	public static void mqttPublishParam1Stream(String runId) {
		/*
		 * Sample code to continually publish messages
		 */
		try {
			param1Points = Files.readAllLines(Paths.get("/home/MQTT/param1.txt"));
			//param2Points = Files.readAllLines(Paths.get("src/main/resources/param2.txt"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new TimerTask() {
			int count = 0;
			@Override
			public void run() {
				MqttMessage message = new MqttMessage(StringValue.newBuilder()
						.setValue(param1Points.get(count)).build().toByteArray());
				message.setQos(0);
				try {
					asyncClient.publish("run/param1/" + runId, message);
				} catch (MqttException e) {
					logger.error("Exception At: ", e);
				}
				count++;
				if (count == param1Points.size()) {
					count = 0;
				}
			}
		}, 10000, 10000); // Every 10 seconds

	}

	public static void mqttPublishParam2Stream(String runId) {
		/*
		 * Sample code to continually publish messages
		 */
		try {
			param2Points = Files.readAllLines(Paths.get("/home/MQTT/param2.txt"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		Timer timer2 = new Timer();
		timer2.scheduleAtFixedRate(new TimerTask() {
			int count = 0;
			@Override
			public void run() {
				MqttMessage message = new MqttMessage(StringValue.newBuilder()
						.setValue(param2Points.get(count)).build().toByteArray());
				message.setQos(0);
				try {
					asyncClient.publish("run/param2/" + runId, message);
				} catch (MqttException e) {
					logger.error("Exception At: ", e);
				}
				count++;
				if (count == param2Points.size()) {
					count = 0;
				}
			}
		}, 30000, 30000); // Every 30 seconds

	}

	public static void mqttPublishUsersAck() {
		try {
			MqttMessage message = new MqttMessage(AmsSyncAck.newBuilder()
					.setClientId(Long.parseLong(asyncClient.getClientId())).setReceived(true).build().toByteArray());
			message.setQos(0);
			asyncClient.publish("sync/ack/users", message);
			logger.info("users ack sent to platform..");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			mqttSchPublish();
		} catch (MqttException e) {
			logger.error("msg: " + e.getMessage());
			logger.error("cause: " + e.getCause());
			e.printStackTrace();
		}
	}

	public static void mqttPublishScheduleAck() {
		try {
			MqttMessage message = new MqttMessage(AmsSyncAck.newBuilder()
					.setClientId(Long.parseLong(asyncClient.getClientId())).setReceived(true).build().toByteArray());
			message.setQos(0);
			asyncClient.publish("sync/ack/schedules", message);
			logger.info("schedule ack sent to platform..");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			mqttProcessPublish();
		} catch (MqttException e) {
			logger.error("msg: " + e.getMessage());
			logger.error("cause: " + e.getCause());
			e.printStackTrace();
		}
	}

	public static void mqttSchPublish() {
		try {
			SyncRequest schSyncRequest = SyncRequest.newBuilder().setClientId(Long.parseLong(asyncClient.getClientId()))
					.build();
			MqttMessage message = new MqttMessage(schSyncRequest.toByteArray());
			message.setQos(0);
			logger.info("sending schedules request...");
			asyncClient.publish("sync/schedules/request", message);
		} catch (MqttException e) {
			logger.error("msg: " + e.getMessage());
			logger.error("cause: " + e.getCause());
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public static void mqttProcessPublish() {
		try {
			AmsProcess.Builder amsProcess = AmsProcess.newBuilder();
			amsProcess.setClientId(Long.parseLong(asyncClient.getClientId()));
			MqttMessage message = new MqttMessage(amsProcess.build().toByteArray());
			message.setQos(0);
			logger.info("sending process list...");
			asyncClient.publish("sync/process", message);
		} catch (MqttException e) {
			logger.error("msg: " + e.getMessage());
			logger.error("cause: " + e.getCause());
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public static void mqttPublishProcessAck() {
		try {
			MqttMessage message = new MqttMessage(AmsSyncAck.newBuilder()
					.setClientId(Long.parseLong(asyncClient.getClientId())).setReceived(true).build().toByteArray());
			message.setQos(0);
			asyncClient.publish("sync/ack/process", message);
			logger.info("process ack sent to platform..");
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			mqttRunRequest();
		} catch (MqttException e) {
			logger.error("msg: " + e.getMessage());
			logger.error("cause: " + e.getCause());
			e.printStackTrace();
		}
	}

	public static void mqttRunRequest() {
		try {
			RunRequest request = RunRequest.newBuilder().setClientId(Long.parseLong(asyncClient.getClientId())).build();
			MqttMessage message = new MqttMessage(request.toByteArray());
			message.setQos(0);
			logger.info("sending run request...");
			asyncClient.publish("run/request", message);
		} catch (MqttException e) {
			logger.error("msg: " + e.getMessage());
			logger.error("cause: " + e.getCause());
			e.printStackTrace();
			System.exit(-1);
		}
	}

}