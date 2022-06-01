package pers.zns.bigdata.learning.models;

import lombok.Data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.io.Serializable;

/**
 * @program: bigdata-learning
 * @description:
 * @author: zns
 * @create: 2022-05-31 17:59
 */
@Data
@JsonPropertyOrder({"mmsi", "imo", "callSign", "shipName", "shipType", "shipLength", "shipBreadth", "fixingDevice", "eta", "draft", "destination", "deviceType", "countryCode", "receiveTime", "sourceId", "toBow", "toStern", "toPort", "toStarboard", "messageType", "baseStationId", "mixShipType", "dwt", "gt", "originalType", "shipTypeLabel", "shipTypeEnLabel"})
public class StaticModel implements Serializable {
    private String id;
    /**
     *
     */
    private String mmsi;

    /**
     *
     */
    private String imo;

    /**
     *
     */
    private String callSign;

    /**
     *
     */
    private String shipName;

    /**
     *
     */
    private String shipType;

    /**
     *
     */
    private String shipLength;

    /**
     *
     */
    private String shipBreadth;

    /**
     *
     */
    private String fixingDevice;

    /**
     *
     */
    private String eta;

    /**
     *
     */
    private String draft;

    /**
     *
     */
    private String destination;

    /**
     *
     */
    private String deviceType;

    /**
     *
     */
    private String countryCode;

    /**
     *
     */
    private String receiveTime;

    /**
     *
     */
    private String sourceId;

    /**
     *
     */
    private String toBow;

    /**
     *
     */
    private String toStern;

    /**
     *
     */
    private String toPort;

    /**
     *
     */
    private String toStarboard;

    /**
     *
     */
    private String messageType;

    /**
     *
     */
    private String baseStationId;

    /**
     *
     */
    private String mixShipType;

    /**
     *
     */
    private String dwt;

    /**
     *
     */
    private String gt;

    /**
     *
     */
    private String originalType;

    /**
     *
     */
    private String shipTypeLabel;

    /**
     *
     */
    private String shipTypeEnLabel;
}