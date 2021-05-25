CREATE TABLE IF NOT EXISTS usertype (
    id INTEGER NOT NULL PRIMARY KEY, 
    name TEXT NOT NULL, 
    description TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS user (
    id INTEGER NOT NULL PRIMARY KEY, 
    name TEXT NOT NULL, 
    user_type_id INTEGER NOT NULL REFERENCES usertype (id), 
    user_schema JSON NOT NULL, 
    version INTEGER NOT NULL, 
    timestamp DATETIME NOT NULL, 
    user_id INTEGER, 
);

CREATE TABLE IF NOT EXISTS assettype (
    id INTEGER NOT NULL PRIMARY KEY, 
    name TEXT NOT NULL, 
    description TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS asset (
    id INTEGER NOT NULL PRIMARY KEY, 
    name TEXT NOT NULL, 
    asset_type_id INTEGER REFERENCES assettype (id), 
    version INTEGER NOT NULL, 
    timestamp DATETIME NOT NULL, 
    user_id INTEGER REFERENCES user (id)
);

CREATE TABLE IF NOT EXISTS whoprofile (
    id INTEGER NOT NULL PRIMARY KEY, 
    version INTEGER NOT NULL, 
    timestamp DATETIME NOT NULL, 
    write_user_id INTEGER REFERENCES user (id), 
    asset_id INTEGER NOT NULL REFERENCES asset (id), 
    user_id INTEGER REFERENCES user (id),
    whoprofile_schema JSON NOT NULL
);

CREATE TABLE IF NOT EXISTS whatprofile (
    id INTEGER NOT NULL PRIMARY KEY, 
    version INTEGER NOT NULL, 
    timestamp DATETIME NOT NULL, 
    user_id INTEGER REFERENCES user (id), 
    asset_id INTEGER NOT NULL REFERENCES asset (id), 
    whatprofile_schema JSON NOT NULL
);

CREATE TABLE IF NOT EXISTS howprofile (
    id INTEGER NOT NULL PRIMARY KEY, 
    version INTEGER NOT NULL, 
    timestamp DATETIME NOT NULL, 
    user_id INTEGER REFERENCES user (id), 
    asset_id INTEGER NOT NULL REFERENCES asset (id), 
    howprofile_schema JSON NOT NULL
);

CREATE TABLE IF NOT EXISTS whyprofile (
    id INTEGER NOT NULL PRIMARY KEY, 
    version INTEGER NOT NULL, 
    timestamp DATETIME NOT NULL, 
    user_id INTEGER REFERENCES user (id), 
    asset_id INTEGER NOT NULL REFERENCES asset (id), 
    whyprofile_schema JSON NOT NULL
);

CREATE TABLE IF NOT EXISTS whenprofile (
    id INTEGER NOT NULL PRIMARY KEY, 
    version INTEGER NOT NULL, 
    timestamp DATETIME NOT NULL, 
    user_id INTEGER REFERENCES user (id), 
    asset_id INTEGER NOT NULL REFERENCES asset (id), 
    asset_timestamp DATETIME, 
    expiry_date DATETIME, 
    start_date DATETIME
);

CREATE TABLE IF NOT EXISTS sourcetype (
    id INTEGER NOT NULL PRIMARY KEY, 
    connector TEXT NOT NULL, 
    serde TEXT NOT NULL, 
    datamodel TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS source (
    id INTEGER NOT NULL PRIMARY KEY, 
    version INTEGER NOT NULL, 
    timestamp DATETIME NOT NULL, 
    user_id INTEGER REFERENCES user (id), 
    name TEXT NOT NULL, 
    source_type_id INTEGER NOT NULL REFERENCES sourcetype (id),
    source_schema JSON NOT NULL
);

CREATE TABLE IF NOT EXISTS whereprofile (
    id INTEGER NOT NULL PRIMARY KEY, 
    version INTEGER NOT NULL, 
    timestamp DATETIME NOT NULL, 
    user_id INTEGER REFERENCES user (id), 
    asset_id INTEGER NOT NULL REFERENCES asset (id), 
    access_path TEXT, 
    source_id INTEGER REFERENCES source (id), 
    configuration JSON
);

CREATE TABLE IF NOT EXISTS action (
    id INTEGER NOT NULL PRIMARY KEY, 
    version INTEGER NOT NULL, 
    timestamp DATETIME NOT NULL, 
    user_id INTEGER REFERENCES user (id), 
    asset_id INTEGER NOT NULL REFERENCES asset (id), 
    who_id INTEGER NOT NULL REFERENCES whoprofile (id), 
    how_id INTEGER NOT NULL REFERENCES howprofile (id), 
    why_id INTEGER NOT NULL REFERENCES whyprofile (id), 
    when_id INTEGER NOT NULL REFERENCES whenprofile (id)
);

CREATE TABLE IF NOT EXISTS relationshiptype (
    id INTEGER NOT NULL PRIMARY KEY, 
    name TEXT NOT NULL, 
    description TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS relationship (
    id INTEGER NOT NULL PRIMARY KEY, 
    version INTEGER NOT NULL, 
    timestamp DATETIME NOT NULL, 
    user_id INTEGER REFERENCES user (id), 
    relationship_type_id INTEGER NOT NULL REFERENCES relationshiptype (id), 
    relationship_schema JSON NOT NULL
);

CREATE TABLE IF NOT EXISTS asset_relationships (
    id INTEGER NOT NULL PRIMARY KEY, 
    asset_id INTEGER NOT NULL REFERENCES asset (id), 
    relationship_id INTEGER NOT NULL REFERENCES relationship (id)
);

----

drop table if exists profiling;
create table if not exists profiling --handle as an event
(
    id                         integer     not null primary key,
    source_id                  integer references data_asset (id),
    who_id                     integer references user (id),
    started_at                 datetime not null,
    source_schema              json default '[]',
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
