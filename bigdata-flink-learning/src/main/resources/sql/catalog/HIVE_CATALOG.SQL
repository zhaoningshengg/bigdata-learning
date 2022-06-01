create
catalog
${hive.catalog.name}
with (
    'type' = 'hive',
    'default-database' = '${hive.catalog.database}',
    'hive-conf-dir' = '${hive.conf.dir}'
);
-- set the hivecatalog as the current catalog of the session
use catalog ${hive.catalog.name};

-- create database if not exists ${hive.catalog.name}.${hive.catalog.database};
-- use ${hive.catalog.database};
