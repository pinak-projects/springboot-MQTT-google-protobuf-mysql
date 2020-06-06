-- Database: mqtt_db
-- ------------------------------------------------------
USE `mqtt_db`;

/*Table structure for table `ams_user_sync` */

CREATE TABLE IF NOT EXISTS `ams_user_sync` (
  `ams_user_sync_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `client_id` bigint(20) NOT NULL,
  `last_synced_on` timestamp  NOT NULL,
  PRIMARY KEY (`ams_user_sync_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

/*Table structure for table `ams_schedule_sync` */

CREATE TABLE IF NOT EXISTS `ams_schedule_sync` (
  `ams_sch_sync_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `client_id` bigint(20) NOT NULL,
  `last_synced_on` timestamp  NOT NULL,
  PRIMARY KEY (`ams_sch_sync_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

/*Table structure for table `ams_process_sync` */

CREATE TABLE IF NOT EXISTS `ams_process_sync` (
  `ams_pro_sync_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `client_id` bigint(20) NOT NULL,
  `last_synced_on` timestamp  NULL DEFAULT NULL,
  `is_synced` boolean NOT NULL DEFAULT false,
  PRIMARY KEY (`ams_pro_sync_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;