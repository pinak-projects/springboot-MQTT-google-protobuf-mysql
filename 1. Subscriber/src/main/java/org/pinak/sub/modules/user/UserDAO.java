package org.pinak.sub.modules.user;

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
import org.pinak.sub.model.UserSyncResponseProto.UserSyncResponse;
import org.pinak.sub.model.UserSyncResponseProto.UserSyncResponse.User;
import org.pinak.sub.model.UserSyncResponseProto.UserSyncResponse.User.Manager;
import org.pinak.sub.model.UserSyncResponseProto.UserSyncResponse.User.Roles;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
public final class UserDAO {

	private UserDAO() {
	}
	
	public static void getUsersService(long clientId) {
		final Connection con = DBConnection.getConnection();
		PreparedStatement getUsersStatement = null;
		PreparedStatement existingAmsUserSync = null;
		PreparedStatement getUserRoles = null;
		PreparedStatement getUserManager = null;
		ResultSet amsResultSets = null;
		ResultSet resultSet = null;
		ResultSet rolesResultSet = null;
		ResultSet managerResultSet = null;
		boolean published = true;
		try {
			con.setAutoCommit(false);
			existingAmsUserSync = con.prepareStatement(ResourceManager.getQueryValue("QUERY_FETCH_EXISTING_AMS_USER_SYNC_LOG"));
			existingAmsUserSync.setLong(1, clientId);
			log.info(existingAmsUserSync.toString());
			amsResultSets = existingAmsUserSync.executeQuery();
			if (amsResultSets.first()) {
				con.commit();
				// ams user sync log exists
				getUsersStatement = con.prepareStatement(ResourceManager.getQueryValue("QUERY_FETCH_UNSYNCED_USERS"));
				getUsersStatement.setTimestamp(1, amsResultSets.getTimestamp(Constants.LAST_SYNCED_ON));
			} else {
				con.commit();
				// new ams device
				getUsersStatement = con.prepareStatement(ResourceManager.getQueryValue("QUERY_FETCH_ALL_USERS"));
			}
			log.info(getUsersStatement.toString());
			resultSet = getUsersStatement.executeQuery();
			if (resultSet.first()) {
				resultSet.beforeFirst();
				UserSyncResponse.Builder usersList = UserSyncResponse.newBuilder();
				while (resultSet.next()) {
					getUserRoles = con.prepareStatement(ResourceManager.getQueryValue("QUERY_FETCH_USER_ROLES"));
					getUserRoles.setLong(1, resultSet.getLong(Constants.COLUMN_USER_ID));
					log.info(getUserRoles.toString());
					rolesResultSet = getUserRoles.executeQuery();
					if (rolesResultSet.first()) {
						rolesResultSet.beforeFirst();
						User.Builder user = User.newBuilder()
								.setUserId(resultSet.getLong(Constants.COLUMN_USER_ID))
								.setUsername(resultSet.getString(Constants.COLUMN_USER_NAME))
								.setFullname(resultSet.getString(Constants.COLUMN_USER_FULL_NAME))
								.setDepartment(resultSet.getString(Constants.COLUMN_USER_DEPARTMENT))
								.setDesignation(resultSet.getString(Constants.COLUMN_USER_DESIGNATION))
								.setEmail(resultSet.getString(Constants.COLUMN_EMAIL_ID))
								.setPhone(resultSet.getString(Constants.COLUMN_USER_PHONE))
								.setCountryCode(resultSet.getString(Constants.COLUMN_USER_PHONE_COUNTRY_CODE))
								.setPassword(resultSet.getString(Constants.COLUMN_PASSWORD))
								.setStatus(resultSet.getString(Constants.COLUMN_STATUS))
								.setCreatedOn(resultSet.getString(Constants.CREATED_ON))
								.setUpdatedOn(resultSet.getString(Constants.UPDATED_ON));
						long managerId = resultSet.getLong(Constants.COLUMN_MANAGER_ID);
						if (managerId > 0) {
							getUserManager = con.prepareStatement(ResourceManager.getQueryValue("QUERY_FETCH_USER_MANAGER"));
							getUserManager.setLong(1, managerId);
							log.info(getUserManager.toString());
							managerResultSet = getUserManager.executeQuery();
							if (managerResultSet.first()) {
								Manager manager = Manager.newBuilder()
										.setUserId(managerResultSet.getLong(Constants.COLUMN_USER_ID))
										.setUsername(managerResultSet.getString(Constants.COLUMN_USER_NAME))
										.setFullname(managerResultSet.getString(Constants.COLUMN_USER_FULL_NAME))
										.setEmail(managerResultSet.getString(Constants.COLUMN_EMAIL_ID))
										.setPhone(managerResultSet.getString(Constants.COLUMN_USER_PHONE))
										.setCountryCode(managerResultSet.getString(Constants.COLUMN_USER_PHONE_COUNTRY_CODE))
										.build();
								user.setManager(manager);
							} else {
								con.rollback();
								log.info("Error in fetching manager details...");
							}
						} else {
							user.setManager(Manager.newBuilder().build());
						}
						Roles.Builder roles = Roles.newBuilder();
						while (rolesResultSet.next()) {
							roles
							.setRoleId(rolesResultSet.getLong(Constants.COLUMN_ROLE_ID))
							.setRoleDesc(rolesResultSet.getString(Constants.COLUMN_USER_ROLE_DESC));
						}
						user.addRoles(roles);
						usersList.addUser(user);
					} else {
						con.rollback();
						log.info("User role(s) not found.");
					}
				}
				// publish user list to the ams client
				 published = UserPublishUtil.mqttPublishUsers(MqttUtil.mqttAsyncClient, usersList.build().toByteArray(), clientId);
			} else {
				// user(s) already synced
				 published = PublishResponse.mqttPublishMessage(MqttUtil.mqttAsyncClient, ResponseMessage.newBuilder()
						 .setMessage("User(s) already synced to device.")
						 .build()
						 .toByteArray(),"response/users/"+clientId);
			}
			if (published) {
				log.info("User(s) published to ams successfully..");
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
			DBConnection.closeConnection(con, getUserRoles, rolesResultSet);
			DBConnection.closeConnection(con, getUsersStatement,resultSet);
			DBConnection.closeConnection(con, existingAmsUserSync, amsResultSets);
			DBConnection.closeConnection(con, getUserManager, managerResultSet);
		}
	}
}