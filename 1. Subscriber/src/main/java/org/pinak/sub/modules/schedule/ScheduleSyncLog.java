package org.pinak.sub.modules.schedule;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.pinak.sub.conn.manager.DBConnection;
import org.pinak.sub.conn.manager.ResourceManager;
import org.pinak.sub.constants.Constants;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
public final class ScheduleSyncLog {

	private ScheduleSyncLog() {
	}
	
	public static void saveAmsScheduleSyncLog(long clientId, Timestamp timestamp) {
		final Connection con = DBConnection.getConnection();
		PreparedStatement updateSyncLogStmt = null;
		PreparedStatement existingAmsScheduleSync = null;
		ResultSet amsResultSets = null;
		int result = 0;
		try {
			con.setAutoCommit(false);
			existingAmsScheduleSync = con.prepareStatement(ResourceManager.getQueryValue("QUERY_FETCH_EXISTING_AMS_SCHEDULE_SYNC_LOG"));
			existingAmsScheduleSync.setLong(1, clientId);
			log.info(existingAmsScheduleSync.toString());
			amsResultSets = existingAmsScheduleSync.executeQuery();
			if (amsResultSets.first()) {
				// update existing record
				updateSyncLogStmt = con.prepareStatement(ResourceManager.getQueryValue("UPDATE_AMS_SCHEDULE_SYNC_LOG"));
				updateSyncLogStmt.setTimestamp(1, timestamp);
				updateSyncLogStmt.setLong(2, clientId);
			} else {
				// new insert in ams_schedule_sync table
				updateSyncLogStmt = con.prepareStatement(ResourceManager.getQueryValue("INSERT_AMS_SCHEDULE_SYNC_LOG"));
				updateSyncLogStmt.setLong(1, clientId);
				updateSyncLogStmt.setTimestamp(2, timestamp);
			}
			log.info(updateSyncLogStmt.toString());
			result = updateSyncLogStmt.executeUpdate();
			if (result == 1) {
				con.commit();
				log.info("AMS schedule sync info successfully saved.");
			} else {
				con.rollback();
				log.error("Ams sync log info not saved.");
			}
		} catch (Exception e) {
			try {
				con.rollback();
			} catch (SQLException e2) {
				log.error(Constants.EXCEPTION, e2);
			}
			log.error(Constants.EXCEPTION, e);
		} finally {
			DBConnection.closeConnection(con, updateSyncLogStmt, null);
			DBConnection.closeConnection(con, existingAmsScheduleSync, amsResultSets);
		}
	}
}