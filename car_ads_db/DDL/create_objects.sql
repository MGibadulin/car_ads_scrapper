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
    price_min           int not null,
    page_size           tinyint not null,
    year                smallint not null,
    page_num            smallint not null,
    process_log_id      int not null,
    insert_date         datetime not null default current_timestamp
);

create table if not exists process_log
(
    process_log_id      int not null primary key auto_increment,
    
    process_desc        varchar(255) not null,
    `user`              varchar(255),
    host                varchar(255),
    start_date          datetime not null default current_timestamp,
    end_date            datetime
);

create table if not exists ads_archive
(
    row_id                       int not null primary key auto_increment,

    ads_id                       int not null,
    source_id                    varchar(100) not null,
    card_url                     varchar(255) not null,
    ad_group_id                  int not null,
    insert_process_log_id        int,
    insert_date                  datetime not null,
    change_status_process_log_id int,
    ad_status                    tinyint not null,
    change_status_date           datetime not null default current_timestamp
);

set @exist := (select count(*) from information_schema.statistics where table_name = 'ads' and index_name = 'ix_ads_ad_group_id' and table_schema = database());
set @sqlstmt := if( @exist > 0, 'select ''INFO: Index already exists.''', 'create index ix_ads_ad_group_id on ads ( ad_group_id );');
prepare stmt from @sqlstmt;
execute stmt;

set @exist := (select count(*) from information_schema.statistics where table_name = 'ads' and index_name = 'ix_ads_source_id_card_url_ad_status_change_status_date' and table_schema = database());
set @sqlstmt := if( @exist > 0, 'select ''INFO: Index already exists.''', 'create unique index ix_ads_source_id_card_url_ad_status_change_status_date on ads(card_url, source_id, ad_status, change_status_date);');
prepare stmt from @sqlstmt;
execute stmt;

set @exist := (select count(*) from information_schema.statistics where table_name = 'ads' and index_name = 'ix_ads_ad_status' and table_schema = database());
set @sqlstmt := if( @exist > 0, 'select ''INFO: Index already exists.''', 'create index ix_ads_ad_status on ads(ad_status);');
prepare stmt from @sqlstmt;
execute stmt;

drop index ix_ads_source_id_card_url_ad_status on ads;

