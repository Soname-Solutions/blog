create or replace table etl_control(
    data_load_id int auto_increment,
    file_nm varchar(360) not null,
    created_at timestamp default current_timestamp,
    primary key (data_load_id)
);