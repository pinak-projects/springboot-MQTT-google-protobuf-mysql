package com.pinak.pub.mqtt;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.pinak.pub.model.PlatformProcessProto.PlatformProcess;
import com.pinak.pub.model.ResponseMessageProto.ResponseMessage;
import com.pinak.pub.model.RunResponseProto.RunResponse;
import com.pinak.pub.model.ScheduleSyncResponseProto.ScheduleSyncResponse;
import com.pinak.pub.model.UserSyncResponseProto.UserSyncResponse;

/**
 * This is the MQTT Callback class which overrides the MQTT Call back methods
 *
 */
class SimpleCallback implements MqttCallbackExtended {

	private static final Logger logger = LogManager.getLogger(SimpleCallback.class);
	
	// Called when the client lost the connection to the broker
	@Override
	public void connectionLost(Throwable arg0) {
		logger.info("Connection Lost: " + arg0.getMessage());
	}

	// Called when a new message has arrived
	@Override
	public void messageArrived(String topic, MqttMessage message) throws InvalidProtocolBufferException {
		logger.info("\nReceived a Message!" +
		        "\n\tTopic:   " + topic + "\n");
		if (topic.contentEquals("response/users/1002")) {
			ResponseMessage responseMessage = ResponseMessage.parseFrom(message.getPayload());
			System.out.println("Response Message: "+ responseMessage.getMessage());
			MqttUtil.mqttPublishUsersAck();
		}
		if (topic.contentEquals("response/schedules/1002")) {
			ResponseMessage responseMessage = ResponseMessage.parseFrom(message.getPayload());
			System.out.println("Response Message: "+ responseMessage.getMessage());
			MqttUtil.mqttPublishScheduleAck();
		}
		if (topic.contentEquals("response/process/1002")) {
			ResponseMessage responseMessage = ResponseMessage.parseFrom(message.getPayload());
			System.out.println("Response Message: "+ responseMessage.getMessage());
			MqttUtil.mqttPublishProcessAck();
		}
		else if(topic.contentEquals("ams/sync/users/1002"))
		{
			UserSyncResponse userSyncResponse = UserSyncResponse.parseFrom(message.getPayload());
			System.out.println("Received: "+ userSyncResponse.toString());
			MqttUtil.mqttPublishUsersAck();
		}
		else if(topic.contentEquals("ams/sync/schedules/1002"))
		{
			ScheduleSyncResponse schSyncResponse = ScheduleSyncResponse.parseFrom(message.getPayload());
			System.out.println("Received: "+ schSyncResponse.toString());
			MqttUtil.mqttPublishScheduleAck();
		}
		else if(topic.contentEquals("ams/sync/process/1002"))
		{
			PlatformProcess platformProcess = PlatformProcess.parseFrom(message.getPayload());
			System.out.println("Received: "+ platformProcess.toString());
			MqttUtil.mqttPublishProcessAck();
		}
		else if(topic.contentEquals("ams/run/response/1002"))
		{
			RunResponse runResponse = RunResponse.parseFrom(message.getPayload());
			System.out.println("Received: "+ runResponse.toString());
			MqttUtil.mqttPublishParam1Stream(runResponse.getRunId());
			new Thread(new Runnable() {
			    public void run() {
			    	MqttUtil.mqttPublishParam2Stream(runResponse.getRunId());			
			    }
			}).start();
		}
	}

	/**
	 * 
	 * deliveryComplete
	 * This callback is invoked when a message published by this client
	 * is successfully received by the broker.
	 * 
	 */
	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {
		// logger.info("Delivery is Complete");
	}
	
	@Override
	public void connectComplete(boolean reconnect, String serverURI) {
		// Make or re-make subscriptions here
        if(reconnect){
        	logger.info("Automatically Reconnected to Broker: "+ serverURI);
        	
        } else {
        	logger.info("Connected for the first time To Broker: "+ serverURI);
        }		
	}
}