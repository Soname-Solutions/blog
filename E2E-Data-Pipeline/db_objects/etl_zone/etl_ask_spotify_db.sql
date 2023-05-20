CREATE TABLE `etl_control` (
  `data_load_id` int PRIMARY KEY AUTO_INCREMENT,
  `file_name` varchar(360) NOT NULL,
  `created_ts` timestamp DEFAULT (current_timestamp)
);
