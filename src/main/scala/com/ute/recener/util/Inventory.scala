package com.ute.recener.util

import java.util.Date

object Inventory {

  def findMeasuringPointsByGroup(group: String): String = {


    s"""(SELECT MEASURING_POINT.ID, MEASURING_POINT.CODE
       FROM MEASURING_POINT@bdmdmprd, MEASURING_POINT_GROUPMPOINT@bdmdmprd, MEASURING_POINT_GROUP@bdmdmprd
       WHERE  MEASURING_POINT_GROUP.CODE=${"'"+group+"'"} AND MEASURING_POINT.ID = MEASURING_POINT_GROUPMPOINT.MEASURING_POINT
       AND MEASURING_POINT_GROUP.ID = MEASURING_POINT_GROUPMPOINT.MEASURING_POINT_GROUP)"""
  }

  def findMeasuringPointsByGroupAndPhase(group: String, phase: Int): String = {


    s"""(SELECT MEASURING_POINT.ID, MEASURING_POINT.CODE
       FROM MEASURING_POINT@bdmdmprd, MEASURING_POINT_GROUPMPOINT@bdmdmprd, MEASURING_POINT_GROUP@bdmdmprd,
       MEASURING_POINT_TECHNICAL_DATA@bdmdmprd
       WHERE  MEASURING_POINT_GROUP.CODE=${"'"+group+"'"} AND MEASURING_POINT.ID = MEASURING_POINT_GROUPMPOINT.MEASURING_POINT
       AND MEASURING_POINT_GROUP.ID = MEASURING_POINT_GROUPMPOINT.MEASURING_POINT_GROUP
       AND MEASURING_POINT.ID=MEASURING_POINT_TECHNICAL_DATA.MEASURING_POINT
       AND MEASURING_POINT_TECHNICAL_DATA.PHASE_TYPE=""" + phase + """)"""
  }

  def findMeasuringPointsBySource(source: Int): String = {


    s"""(SELECT MEASURING_POINT.ID, MEASURING_POINT.CODE
       FROM MDMEXP.MEASURING_POINT@bdmdmprd
       WHERE  MEASURING_POINT.DEFAULT_SOURCE=${"'"+source+"'"})"""
  }

  def findIndirectMeasuringPoints(): String = {

    s"""(SELECT DISTINCT(MP.ID)
         FROM MEASURING_POINT@bdmdmprd                MP,
              EQUIPMENT_POINT_CONFIGURATION@bdmdmprd  EC
        WHERE EC.MEASURING_POINT = MP.ID
          AND EC.READING_CONSTANT > 1)"""
  }

  def findIndirectMeasuringPointsInfo(): String = {

    s"""(SELECT MP.ID, MP.CODE, O.MANAGEMENT, O.CODE AS OFFICE, T.CODE AS TARIFF_STRUCTURE,
              C.START_DATE AS SA_START_DATE, C.END_DATE AS SA_END_DATE,
              B.TARIFF_STRUCTURE_START_DATE,  B.TARIFF_STRUCTURE_END_DATE,
              VL.CODE AS VOLTAGE_LEVEL
         FROM MEASURING_POINT@bdmdmprd                MP,
              MEASURING_POINT_LOCATION_DATA@bdmdmprd  LD,
              OWNER@bdmdmprd                          O,
              MEASURING_POINT_COMMERCIAL@bdmdmprd     C,
              MEASURING_POINT_BILLING_DATA@bdmdmprd   B,
              TARIFF_STRUCTURE@bdmdmprd               T,
              MEASURING_POINT_TECHNICAL_DATA@bdmdmprd MPTT,
              VOLTAGE_LEVEL@bdmdmprd                  VL
        WHERE MP.ID IN (SELECT MP.ID
                          FROM MEASURING_POINT@bdmdmprd                MP,
                               EQUIPMENT_POINT_CONFIGURATION@bdmdmprd  EC
                         WHERE EC.MEASURING_POINT = MP.ID
                           AND EC.READING_CONSTANT > 1)
          AND MP.LOCATION_DATA = LD.ID
          AND LD.OWNER = O.ID
          AND MP.ID = C.MEASURING_POINT
          AND C.ID = B.COMMERCIAL_DATA
          AND B.TARIFF_STRUCTURE = T.ID
          AND MPTT.MEASURING_POINT = MP.ID
          AND MPTT.VOLTAGE_LEVEL = VL.ID)"""
  }

  def findIndirectMeasuringPointsEquipments(): String = {

    s"""(SELECT MP.ID, EC.START_DATE AS EQUIP_START_DATE, EC.END_DATE AS EQUIP_END_DATE, E.CODE AS BADGE_NUMBER
         FROM MEASURING_POINT@bdmdmprd                MP,
              EQUIPMENT_POINT_CONFIGURATION@bdmdmprd  EC,
              EQUIPMENT@bdmdmprd                      E
        WHERE EC.MEASURING_POINT = MP.ID
          AND EC.EQUIPMENT = E.ID
          AND MP.ID In (SELECT DISTINCT(MP.ID)
         FROM MEASURING_POINT@bdmdmprd                MP,
              EQUIPMENT_POINT_CONFIGURATION@bdmdmprd  EC
        WHERE EC.MEASURING_POINT = MP.ID
          AND EC.READING_CONSTANT > 1))"""
  }

  def findSources(): String = {

    s"""(SELECT ID AS SOURCE_ID, NAME AS SOURCE_NAME FROM SOURCE@bdmdmprd)"""

  }

  def findMagnitudes(): String = {

    s"""(SELECT ID AS MAGNITUDE_ID, DESCRIPTION
         FROM MAGNITUDE@bdmdmprd
       UNION
       SELECT ID AS MAGNITUDE_ID, DESCRIPTION
         FROM INSTANTANEOUS_VALUE_MAGNITUDE@bdmdmprd)"""

  }


}
