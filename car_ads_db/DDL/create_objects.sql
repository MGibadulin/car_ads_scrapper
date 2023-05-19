create database if not exists car_ads_db;

use car_ads_db;

create table if not exists ads
(
    ads_id                       int not null primary key auto_increment,
    
    source_id                    varchar(100) not null,
    card_url                     varchar(255) not null,
    ad_group_id                  int not null,
    insert_process_log_id        int,
    insert_date                  datetime not null default current_timestamp,
    change_status_process_log_id int,
    ad_status                    tinyint not null default 0,
    change_status_date           datetime
);

create table if not exists ad_groups
(
    ad_group_id         int not null primary key auto_increment,
    
    group_url           varchar(255) not null,
    process_log_id      int not null,
    insert_date         datetime not null default current_timestamp
);

create table if not exists process_log
(
    process_log_id      int not null primary key auto_increment,
    
    process_desc        varchar(255) not null,
    start_date          datetime not null default current_timestamp,
    end_date            datetime
);

create index ix_ads_ad_group_id on ads(ad_group_id);