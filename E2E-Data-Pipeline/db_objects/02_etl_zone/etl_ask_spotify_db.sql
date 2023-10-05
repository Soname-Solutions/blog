CREATE TABLE `etl_control` (
  `data_load_id` int UNIQUE AUTO_INCREMENT,
  `file_name` varchar(360) PRIMARY KEY NOT NULL,
  `status` varchar(360),
  `created_ts` timestamp DEFAULT (current_timestamp)
);
