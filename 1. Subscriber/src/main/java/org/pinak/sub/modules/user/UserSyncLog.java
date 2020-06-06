package org.pinak.sub.modules.user;

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
public final class UserSyncLog {

	private UserSyncLog() {
	}

	public static void saveAmsUserSyncLog(long clinetId, Timestamp timestamp) {
		final Connection con = DBConnection.getConnection();
		PreparedStatement updateSyncLogStmt = null;
		PreparedStatement existingAmsUserSync = null;
		ResultSet amsResultSets = null;
		int result = 0;
		try {
			con.setAutoCommit(false);
			existingAmsUserSync = con
					.prepareStatement(ResourceManager.getQueryValue("QUERY_FETCH_EXISTING_AMS_USER_SYNC_LOG"));
			existingAmsUserSync.setLong(1, clinetId);
			log.info(existingAmsUserSync.toString());
			amsResultSets = existingAmsUserSync.executeQuery();
			if (amsResultSets.first()) {
				// update existing record
				updateSyncLogStmt = con.prepareStatement(ResourceManager.getQueryValue("UPDATE_AMS_USER_SYNC_LOG"));
				updateSyncLogStmt.setTimestamp(1, timestamp);
				updateSyncLogStmt.setLong(2, clinetId);
			} else {
				// new insert in ams_user_sync table
				updateSyncLogStmt = con.prepareStatement(ResourceManager.getQueryValue("INSERT_AMS_USER_SYNC_LOG"));
				updateSyncLogStmt.setLong(1, clinetId);
				updateSyncLogStmt.setTimestamp(2, timestamp);
			}
			log.info(updateSyncLogStmt.toString());
			result = updateSyncLogStmt.executeUpdate();
			if (result == 1) {
				con.commit();
				log.info("AMS user sync info successfully saved.");
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
			DBConnection.closeConnection(con, existingAmsUserSync, amsResultSets);
		}
	}
}