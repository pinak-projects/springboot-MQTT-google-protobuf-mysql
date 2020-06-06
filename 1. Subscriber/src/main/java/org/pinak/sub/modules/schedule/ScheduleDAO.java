package org.pinak.sub.modules.schedule;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.pinak.sub.config.MqttUtil;
import org.pinak.sub.config.PublishResponse;
import org.pinak.sub.conn.manager.DBConnection;
import org.pinak.sub.conn.manager.ResourceManager;
import org.pinak.sub.constants.Constants;
import org.pinak.sub.model.ResponseMessageProto.ResponseMessage;
import org.pinak.sub.model.ScheduleSyncResponseProto.ScheduleSyncResponse;
import org.pinak.sub.model.ScheduleSyncResponseProto.ScheduleSyncResponse.Schedule;
import org.pinak.sub.model.ScheduleSyncResponseProto.ScheduleSyncResponse.Schedule.Operators;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
public final class ScheduleDAO {

	private ScheduleDAO() {
	}
	
	public static void getSchedulesService(long clientId) {
		final Connection con = DBConnection.getConnection();
		PreparedStatement getSchedulesStatement = null;
		PreparedStatement existingAmsUserSync = null;
		PreparedStatement getOperators = null;
		ResultSet amsResultSets = null;
		ResultSet scheduleResultSet = null;
		ResultSet operatorsResultSet = null;
		boolean published = true;
		try {
			con.setAutoCommit(false);
			existingAmsUserSync = con.prepareStatement(ResourceManager.getQueryValue("QUERY_FETCH_EXISTING_AMS_SCHEDULE_SYNC_LOG"));
			existingAmsUserSync.setLong(1, clientId);
			log.info(existingAmsUserSync.toString());
			amsResultSets = existingAmsUserSync.executeQuery();
			if (amsResultSets.first()) {
				con.commit();
				// ams schedule sync log exists
				getSchedulesStatement = con.prepareStatement(ResourceManager.getQueryValue("QUERY_FETCH_UNSYNCED_SCHEDULES"));
				getSchedulesStatement.setLong(1, clientId);
				getSchedulesStatement.setTimestamp(2, amsResultSets.getTimestamp(Constants.LAST_SYNCED_ON));
			} else {
				con.commit();
				// new ams device
				getSchedulesStatement = con.prepareStatement(ResourceManager.getQueryValue("QUERY_FETCH_ALL_SCHEDULES"));
				getSchedulesStatement.setLong(1, clientId);
			}
			log.info(getSchedulesStatement.toString());
			scheduleResultSet = getSchedulesStatement.executeQuery();
			if (scheduleResultSet.first()) {
				scheduleResultSet.beforeFirst();
				ScheduleSyncResponse.Builder schedulesList = ScheduleSyncResponse.newBuilder();
				while (scheduleResultSet.next()) {
						Schedule.Builder schedule = Schedule.newBuilder()
								.setScheduleId(scheduleResultSet.getLong(Constants.COLUMN_SCHEDULE_ID))
								.setSchStartDate(scheduleResultSet.getLong(Constants.COLUMN_START_DATE_TIME))
								.setSchEndDate(scheduleResultSet.getLong(Constants.COLUMN_END_DATE_TIME))
								.setAmsId(scheduleResultSet.getLong(Constants.COLUMN_AMS_ID))
								.setProcessId(scheduleResultSet.getString(Constants.COLUMN_PROCESS_ID))
								.setProcessTitle(scheduleResultSet.getString(Constants.COLUMN_TITLE))	
								.setCreatedOn(scheduleResultSet.getString(Constants.CREATED_ON))
								.setUpdatedOn(scheduleResultSet.getString(Constants.UPDATED_ON));
						if (scheduleResultSet.getString(Constants.COLUMN_MANAGER_ID) != null) {
							schedule.setManagerId(scheduleResultSet.getLong(Constants.COLUMN_MANAGER_ID));
							schedule.setManagerUsername(scheduleResultSet.getString(Constants.COLUMN_USER_NAME));
						}
						getOperators = con.prepareStatement(ResourceManager.getQueryValue("QUERY_FETCH_USER_OPERATORS"));
						getOperators.setLong(1, scheduleResultSet.getLong(Constants.COLUMN_SCHEDULE_ID));
						log.info(getOperators.toString());
						operatorsResultSet = getOperators.executeQuery();
						if (operatorsResultSet.first()) {
							operatorsResultSet.beforeFirst();
							while (operatorsResultSet.next()) {
								Operators operator = Operators.newBuilder()
								.setUserId(operatorsResultSet.getLong(Constants.COLUMN_USER_ID))
								.setUsername(operatorsResultSet.getString(Constants.COLUMN_USER_NAME))
								.build();
							schedule.addOperators(operator);
							}
						} else {
							schedule.addOperators(Operators.newBuilder().build()); 
						}
						schedulesList.addSchedule(schedule);
				}
				// publish schedule list to the ams client
				 published = SchedulePublishUtil.mqttPublishSchedules(MqttUtil.mqttAsyncClient, schedulesList.build().toByteArray(), clientId);
			} else {
				// schedule(s) already synced
				 published = PublishResponse.mqttPublishMessage(MqttUtil.mqttAsyncClient, ResponseMessage.newBuilder()
						 .setMessage("Schedule(s) already synced to device.")
						 .build()
						 .toByteArray(),"response/schedules/"+clientId);
			}
			if (published) {
				log.info("Schedule(s) published to ams successfully..");
			} else {
				log.info(Constants.SOMETHING_WENT_WRONG);
			}
		} catch (Exception e) {
			try {
				con.rollback();
			} catch (SQLException e2) {
				log.error(Constants.EXCEPTION, e2);
			}
			log.error(Constants.EXCEPTION, e);
		} finally {
			DBConnection.closeConnection(con, getOperators, operatorsResultSet);
			DBConnection.closeConnection(con, getSchedulesStatement, scheduleResultSet);
			DBConnection.closeConnection(con, existingAmsUserSync, amsResultSets);
		}
	}
}