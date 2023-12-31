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
       FROM MEASURING_POINT, MEASURING_POINT_GROUPMPOINT, MEASURING_POINT_GROUP, MEASURING_POINT_TECHNICAL_DATA
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

}
