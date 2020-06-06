package org.pinak.sub.service;

import java.sql.Connection;
import java.sql.PreparedStatement;

import org.json.JSONObject;
import org.pinak.sub.conn.manager.DBConnection;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClientService {

	public JSONObject saveClientToDB(JSONObject connObject) {
		final Connection con = DBConnection.getConnection();
		JSONObject response = new JSONObject();
		PreparedStatement ps = null;
		int result = 0;
		try {
			String query = "insert into client(client_id, conn_ack, start_time) values(?,?,?)";
			ps = con.prepareStatement(query);
			ps.setString(1, connObject.getString("clientid"));
			ps.setInt(2, connObject.getInt("connack"));
			ps.setLong(3, connObject.getLong("ts"));
			log.info(ps.toString());
			result = ps.executeUpdate();
			if (result == 1) {
				response.put("success", "done");
			} else {
				response.put("error", "error");			
				}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnection.closeConnection(con, ps, null);
		}
		return response;
	}

	public JSONObject updateClientEndTime(long endTime, String clientId) {
		final Connection con = DBConnection.getConnection();
		JSONObject response = new JSONObject();
		PreparedStatement ps = null;
		int result = 0;
		try {
			String query = "UPDATE client SET end_time=? where clientid=? ORDER BY id DESC LIMIT 1";
			ps = con.prepareStatement(query);
			ps.setLong(1, endTime);
			ps.setString(2, clientId);
			log.info(ps.toString());
			result = ps.executeUpdate();
			System.out.println(result);
			if (result == 1) {
				response.put("success", "done");
			} else {
				response.put("error", "error");			
				}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnection.closeConnection(con, ps, null);
		}
		return response;
	}
}
