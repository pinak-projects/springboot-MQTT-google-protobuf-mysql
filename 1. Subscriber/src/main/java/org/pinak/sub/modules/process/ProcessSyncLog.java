package org.pinak.sub.modules.process;

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
public final class ProcessSyncLog {

	private ProcessSyncLog() {
	}

	public static void saveAmsProcessSyncLog(long clientId, Timestamp timestamp) {
		final Connection con = DBConnection.getConnection();
		PreparedStatement updateSyncLogStmt = null;
		PreparedStatement existingAmsProcessSync = null;
		ResultSet amsResultSets = null;
		int result = 0;
		try {
			con.setAutoCommit(false);
			existingAmsProcessSync = con
					.prepareStatement(ResourceManager.getQueryValue("QUERY_FETCH_EXISTING_AMS_PROCESS_SYNC_LOG"));
			existingAmsProcessSync.setLong(1, clientId);
			log.info(existingAmsProcessSync.toString());
			amsResultSets = existingAmsProcessSync.executeQuery();
			if (amsResultSets.first()) {
				// update existing record
				updateSyncLogStmt = con.prepareStatement(ResourceManager.getQueryValue("UPDATE_AMS_PROCESS_SYNC_LOG"));
				updateSyncLogStmt.setTimestamp(1, timestamp);
				updateSyncLogStmt.setLong(2, clientId);
			} else {
				// new insert in ams_process_sync table
				updateSyncLogStmt = con.prepareStatement(ResourceManager.getQueryValue("INSERT_AMS_PROCESS_SYNC_LOG"));
				updateSyncLogStmt.setLong(1, clientId);
				updateSyncLogStmt.setTimestamp(2, timestamp);
			}
			log.info(updateSyncLogStmt.toString());
			result = updateSyncLogStmt.executeUpdate();
			if (result == 1) {
				con.commit();
				log.info("Ams process sync log info saved successfully...");
			} else {
				con.rollback();
				log.error("Ams process sync log info not saved.");
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
			DBConnection.closeConnection(con, existingAmsProcessSync, amsResultSets);
		}
	}

	public static boolean countNotSyncedAmsDevices() {
		final Connection con = DBConnection.getConnection();
		PreparedStatement getNotSyncedAmsCount = null;
		ResultSet unsyncedCount = null;
		try {
			con.setAutoCommit(false);
			getNotSyncedAmsCount = con.prepareStatement(ResourceManager.getQueryValue("QUERY_COUNT_NOT_SYNCED_AMS"));
			log.info(getNotSyncedAmsCount.toString());
			unsyncedCount = getNotSyncedAmsCount.executeQuery();
			if (unsyncedCount.first() && unsyncedCount.getInt(Constants.COUNT) == 0) {
				return true;
			}
		} catch (Exception e) {
			try {
				con.rollback();
			} catch (SQLException e2) {
				log.error(Constants.EXCEPTION, e2);
			}
			log.error(Constants.EXCEPTION, e);
		} finally {
			DBConnection.closeConnection(con, getNotSyncedAmsCount, unsyncedCount);
		}
		return false;
	}

	public static boolean unsyncedOtherAmsDevices(long clientId) {
		final Connection con = DBConnection.getConnection();
		PreparedStatement updateAmsSyncFlag = null;
		int result = 0;
		try {
			updateAmsSyncFlag = con
					.prepareStatement(ResourceManager.getQueryValue("UPDATE_OTHER_AMS_PROCESS_SYNC_FLAG"));
			updateAmsSyncFlag.setLong(1, clientId);
			log.info(updateAmsSyncFlag.toString());
			result = updateAmsSyncFlag.executeUpdate();
			if (result > 0) {
				log.info("Other AMS devices set to unsynced successfully.");
				return true;
			} else {
				log.info("Other AMS device(s) does not exists..");
				return true;
			}
		} catch (Exception e) {
			try {
				con.rollback();
			} catch (SQLException e2) {
				log.error(Constants.EXCEPTION, e2);
			}
			log.error(Constants.EXCEPTION, e);
		} finally {
			DBConnection.closeConnection(con, updateAmsSyncFlag, null);
		}
		return false;
	}
}