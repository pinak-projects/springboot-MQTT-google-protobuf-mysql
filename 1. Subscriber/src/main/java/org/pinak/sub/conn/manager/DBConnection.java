package org.pinak.sub.conn.manager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
public final class DBConnection {

	private static HikariDataSource dataSource;

	private DBConnection() {
	}

	private static HikariDataSource getDatasource() {
		if (dataSource == null) {
			HikariConfig hikariConfig = new HikariConfig();
			hikariConfig.setDriverClassName("com.mysql.cj.jdbc.Driver");
			hikariConfig.setJdbcUrl("jdbc:mysql://localhost:3306/millipore_db?useSSL=false");
			hikariConfig.setUsername("root");
			// hikariConfig.setPassword(password);
			hikariConfig.setMaximumPoolSize(12);
			hikariConfig.setConnectionTestQuery("SELECT 1");
			hikariConfig.setPoolName("springHikariCP");
			hikariConfig.setAutoCommit(true);
			hikariConfig.setConnectionTimeout(20000);
			hikariConfig.setMinimumIdle(5);
			hikariConfig.setMaxLifetime(1200000);
			hikariConfig.setIdleTimeout(300000);
			dataSource = new HikariDataSource(hikariConfig);
		}
		return dataSource;
	}

	public static Connection getConnection() {
		getDatasource();
		try {
			return dataSource.getConnection();
		} catch (SQLException e) {
			log.error("Connection failure...", e);
			return null;
		}
	}

	public static void closeConnection(Connection con, PreparedStatement pmst, ResultSet resultSet) {
		try {
			if (con != null && !con.isClosed()) {
				con.close();
			}
			if (pmst != null && !pmst.isClosed()) {
				pmst.close();
			}
			if (resultSet != null && !resultSet.isClosed()) {
				resultSet.close();
			}
		} catch (Exception e) {
			log.info(e.getMessage());
		}
	}

}
