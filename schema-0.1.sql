create table if not exists user
(
    id         text not null
        primary key,
    name       text,
    permission text default 'ADMIN' not null,
    email      text,
    check (permission in ('READ', 'WRITE', 'ADMIN'))
);

drop table data_asset;
create table if not exists data_asset
(
    id         text     not null
        primary key,
    owner_id   text references user (id),
    name       text     not null,
    metadata   JSON,
    created_at datetime not null
);
drop table if exists profiling;
create table if not exists profiling --handle as an event
(
    id                         text     not null primary key,
    source_id                  text references data_asset (id),
    who_id                     text references user (id),
    started_at                 datetime not null,
    source_schema              text,
    source_schema_retrieved_at datetime,
    description                text,
    metadata                   json default '[]',
    finished_at                datetime
);

CREATE VIRTUAL TABLE searchable USING fts5
(
    source_schema,
    profile_metadata,
    user_metadata
);



-- create table howprofile
-- (
--     id       INTEGER not null
--         primary key,
--     asset_id INTEGER not null
--         references asset,
--     schema   JSON    not null
-- );
--
--
-- create table source_type
-- (
--     id        INTEGER not null
--         primary key,
--     connector TEXT    not null,
--     serde     TEXT    not null,
--     datamodel TEXT    not null
-- );
--
-- create table source
-- (
--     id             INTEGER not null
--         primary key,
--     name           TEXT    not null,
--     source_type_id INTEGER not null
--         references sourcetype,
--     schema         JSON    not null
-- );
--
-- create
-- index source_source_type_id
-- 	on source (source_type_id);
--
-- create table usertype
-- (
--     id          INTEGER not null
--         primary key,
--     name        TEXT    not null,
--     description TEXT    not null
-- );
--
-- create table user
-- (
--     id           INTEGER not null
--         primary key,
--     name         TEXT    not null,
--     user_type_id INTEGER not null
--         references usertype,
--     schema       JSON    not null
-- );
--
--
-- create table whatprofile
-- (
--     id       INTEGER not null
--         primary key,
--     asset_id TEXT    not null
--         references asset (name),
--     schema   JSON    not null
-- );
--
-- create
-- index whatprofile_asset_id
-- 	on whatprofile (asset_id);
--
-- create table whenprofile
-- (
--     id              INTEGER  not null
--         primary key,
--     asset_id        INTEGER  not null
--         references asset,
--     asset_timestamp DATETIME not null,
--     expiry_date     DATETIME not null,
--     start_date      DATETIME not null
-- );
--
-- create
-- index whenprofile_asset_id
-- 	on whenprofile (asset_id);
--
-- create table whereprofile
-- (
--     id            INTEGER not null
--         primary key,
--     asset_id      INTEGER not null
--         references asset,
--     access_path   TEXT    not null,
--     source_id     INTEGER not null
--         references source,
--     configuration TEXT    not null
-- );
--
-- create
-- index whereprofile_asset_id
-- 	on whereprofile (asset_id);
--
-- create
-- index whereprofile_source_id
-- 	on whereprofile (source_id);
--
-- create table whoprofile
-- (
--     id       INTEGER not null
--         primary key,
--     asset_id TEXT    not null
--         references asset (name),
--     user_id  TEXT    not null
--         references user (name),
--     schema   JSON    not null
-- );
--
-- create
-- index whoprofile_asset_id
-- 	on whoprofile (asset_id);
--
-- create
-- index whoprofile_user_id
-- 	on whoprofile (user_id);
--
-- create table whyprofile
-- (
--     id       INTEGER not null
--         primary key,
--     asset_id INTEGER not null
--         references asset,
--     schema   JSON    not null
-- );
--
-- create table action
-- (
--     id       INTEGER not null
--         primary key,
--     asset_id INTEGER not null
--         references asset,
--     who_id   INTEGER not null
--         references whoprofile,
--     how_id   INTEGER not null
--         references howprofile,
--     why_id   INTEGER not null
--         references whyprofile,
--     when_id  INTEGER not null
--         references whenprofile
-- );
