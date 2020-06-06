package org.pinak.sub.config;

import java.sql.Timestamp;

import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.pinak.sub.model.AmsProcessProto.AmsProcess;
import org.pinak.sub.model.AmsSyncAckProto.AmsSyncAck;
import org.pinak.sub.model.ResponseMessageProto.ResponseMessage;
import org.pinak.sub.model.RunRequestProto.RunRequest;
import org.pinak.sub.model.SyncRequestProto.SyncRequest;
import org.pinak.sub.modules.process.ProcessDAO;
import org.pinak.sub.modules.process.ProcessSyncLog;
import org.pinak.sub.modules.run.RunService;
import org.pinak.sub.modules.schedule.ScheduleDAO;
import org.pinak.sub.modules.schedule.ScheduleSyncLog;
import org.pinak.sub.modules.user.UserDAO;
import org.pinak.sub.modules.user.UserSyncLog;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.StringValue;
import com.googlecode.protobuf.format.JsonFormat;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
public final class MqttMessageHandler {

	private MqttMessageHandler() {
	}

	public static void runMessageArrived(String topic, MqttMessage message) throws InvalidProtocolBufferException {
		if (topic.contains("param1")) {
			String param1JsonString = new JsonFormat().printToString(StringValue.parseFrom(message.getPayload()));
			log.info(param1JsonString);
			EventHandler.onParam1Data(param1JsonString);
		} else if (topic.contains("param2")) {
			String param2JsonString = new JsonFormat().printToString(StringValue.parseFrom(message.getPayload()));
			log.info(param2JsonString);
			EventHandler.onParam2Data(param2JsonString);
		} else if (topic.contains("request")) {
			log.info("run request received...");
			RunService.generateRunId(RunRequest.parseFrom(message.getPayload()).getClientId());
		} else {
			log.error("Topic " + topic + " not found.");
		}
	}

	public static void syncMessageArrived(String syncTopic, MqttMessage message) throws InvalidProtocolBufferException {
		switch (syncTopic) {
		case "sync/users/request":
			log.info("get users request received...");
			SyncRequest userSyncRequest = SyncRequest.parseFrom(message.getPayload());
			UserDAO.getUsersService(userSyncRequest.getClientId());
			break;
		case "sync/ack/users":
			AmsSyncAck amsUserSyncAck = AmsSyncAck.parseFrom(message.getPayload());
			if (amsUserSyncAck.getReceived()) {
				UserSyncLog.saveAmsUserSyncLog(amsUserSyncAck.getClientId(), new Timestamp(System.currentTimeMillis()));
			} else {
				log.error("User(s) not synced.");
				PublishResponse.mqttPublishMessage(
						MqttUtil.mqttAsyncClient, ResponseMessage.newBuilder()
								.setMessage("Error in syncing user(s), please request again").build().toByteArray(),
						"error/sync/users/" + amsUserSyncAck.getClientId());
			}
			break;
		case "sync/schedules/request":
			log.info("get schedules request received...");
			SyncRequest schSyncRequest = SyncRequest.parseFrom(message.getPayload());
			ScheduleDAO.getSchedulesService(schSyncRequest.getClientId());
			break;
		case "sync/ack/schedules":
			AmsSyncAck amsSyncAck = AmsSyncAck.parseFrom(message.getPayload());
			if (amsSyncAck.getReceived()) {
				ScheduleSyncLog.saveAmsScheduleSyncLog(amsSyncAck.getClientId(),
						new Timestamp(System.currentTimeMillis()));
			} else {
				log.error("Schedule(s) not synced.");
				PublishResponse.mqttPublishMessage(
						MqttUtil.mqttAsyncClient, ResponseMessage.newBuilder()
								.setMessage("Error in syncing schedule(s), please request again").build().toByteArray(),
						"error/sync/schedules/" + amsSyncAck.getClientId());
			}
			break;
		case "sync/process":
			log.info("process(es) received from ams...");
			AmsProcess receivedProcess = AmsProcess.parseFrom(message.getPayload());
			ProcessDAO.getProcessService(receivedProcess);
			break;
		case "sync/ack/process":
			AmsSyncAck processSyncAck = AmsSyncAck.parseFrom(message.getPayload());
			if (processSyncAck.getReceived() && processSyncAck.getClientId() != 0) {
				ProcessSyncLog.saveAmsProcessSyncLog(processSyncAck.getClientId(),
						new Timestamp(System.currentTimeMillis()));
				// check if all the ams devices are synced, if yes then change modified status
				// to 0 in process master
				if (ProcessSyncLog.countNotSyncedAmsDevices()) {
					// all ams devices have been synced, update modified status to 0 in process
					// master
					log.info(
							"All ams devices are synced, setting modified status to 0 for all processes on platform..");
					if (ProcessDAO.updateProcessModifiedStatus()) {
						// modified status changed to 0 for all processes on platform
						log.info("All ams devices successfully synced to platform.");
					}
				} else {
					log.info("All processes on platform already synced to ams devices");
				}
			} else {
				log.error("Process(es) not synced, Something went wrong...");
				PublishResponse.mqttPublishMessage(
						MqttUtil.mqttAsyncClient, ResponseMessage.newBuilder()
								.setMessage("Error in syncing process(es), please request again").build().toByteArray(),
						"error/sync/process/" + processSyncAck.getClientId());
			}
			break;
		default:
			log.error("Topic " + syncTopic + " not found.");
			break;
		}

	}
}
