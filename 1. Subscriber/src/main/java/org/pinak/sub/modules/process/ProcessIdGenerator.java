package org.pinak.sub.modules.process;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.pinak.sub.conn.manager.DBConnection;
import org.pinak.sub.constants.Constants;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProcessIdGenerator {

	private ProcessIdGenerator() {
	}

	public static String generate() {
		final Connection con = DBConnection.getConnection();
		PreparedStatement statement = null;
		ResultSet rs = null;
		String generatedId = null;
		try {
			statement = con.prepareStatement("select max(process_id) from process_master");
			log.info(statement.toString());
			rs = statement.executeQuery();
			if (rs.first() && rs.getString(1) != null) {
				generatedId = Constants.PROCESS_PREFIX
						+ String.format("%03d", Long.parseLong(rs.getString(1).substring(3)) + 1);
			} else {
				generatedId = Constants.PROCESS_PREFIX + String.format("%03d", new Long(1));
			}
		} catch (SQLException e) {
			log.error(Constants.EXCEPTION, e);
		} finally {
			DBConnection.closeConnection(con, statement, rs);
		}
		return generatedId;
	}
}
