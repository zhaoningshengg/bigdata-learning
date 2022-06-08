-- create database test_304;

CREATE TABLE IF NOT EXISTS test_304.static_01
(
    id              varchar(64),
    mmsi            varchar(32),
    imo             varchar(32),
    callSign        varchar(16),
    shipName        varchar(64),
    shipType        varchar(32),
    shipLength      varchar(32),
    shipBreadth     varchar(32),
    fixingDevice    varchar(32),
    eta             varchar(32),
    draft           varchar(32),
    destination     varchar(128),
    deviceType      varchar(8),
    countryCode     varchar(32),
    receiveTime     varchar(32),
    sourceId        varchar(32),
    toBow           varchar(32),
    toStern         varchar(32),
    toPort          varchar(32),
    toStarboard     varchar(32),
    messageType     varchar(32),
    baseStationId   varchar(32),
    mixShipType     varchar(32),
    dwt             varchar(32),
    gt              varchar(32),
    originalType    varchar(8),
    shipTypeLabel   varchar(64),
    shipTypeEnLabel varchar(64)
) UNIQUE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES("replication_num" = "1");