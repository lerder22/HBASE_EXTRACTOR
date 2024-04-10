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

  def   findMeasuringPointsBySource(source: Int): String = {


    s"""(SELECT DISTINCT MEASURING_POINT.ID, MEASURING_POINT.CODE
    FROM MDMEXP.MEASURING_POINT@bdmdmprd left join
     DWHCBPRD.DW_DATOS_PUNTO_SERVICIO@BIPRDHA DT
       ON DT.PUNTO_SERVICIO = MEASURING_POINT.CODE
    WHERE DT.FASES LIKE 'TRI%' AND DT.NUMERO_PLACA LIKE '070%')"""
  }

  def findIndirectMeasuringPoints(): String = {

    s"""(SELECT DISTINCT(MP.ID)
         FROM MEASURING_POINT@bdmdmprd                MP,
              EQUIPMENT_POINT_CONFIGURATION@bdmdmprd  EC
        WHERE EC.MEASURING_POINT = MP.ID
          AND EC.READING_CONSTANT > 1)"""
  }

  def findInspectedMeasuringPoints(): String = {

    s"""(WITH TMP_KAIFA AS (
       |  select DT1.PUNTO_SERVICIO, MED.FECHA_DESDE
       |  from dwhcbprd.dw_datos_punto_servicio@biprdha dt1
       |    LEFT JOIN DWHCBPRD.dw_dt_medidor@biprdha med on med.numero_placa = dt1.numero_placa
       |    where dt1.numero_placa like '070%'
       |),
       |ext_insp as (
       |  select t.*, dense_rank() over (partition by id order by COMMIT_DTTM desc /*to obtain the most recent*/) date_rank
       |  from deptorecener.dt_extraction_uruguay t
       |  where to_date(fecha, 'dd/mm/yyyy') IN (select max(to_date(fecha, 'dd/mm/yyyy')) from deptorecener.dt_extraction_uruguay)
       |        AND t.FECHAI1 IS NOT NULL
       |        -- AND ROWNUM < 10000
       |), tmp_pm as(
       |  SELECT DISTINCT ID, CODE
       |  FROM MEASURING_POINT@bdmdmprd MP
       |)
       |select DISTINCT PM.ID PUNTO_SERVICIO, pm.code PUNTO_SERVICIO_CCB, T.FECHAI1
       |from tmp_pm pm left join ext_insp t on pm.CODE = T.ID
       |where fechai1 is not null
       |  and t.id in (
       |      SELECT PUNTO_SERVICIO FROM  TMP_KAIFA KF
       |      WHERE to_date(T.fechaI1, 'DD/MM/YYYY') >= KF.FECHA_DESDE
       |  ))""".stripMargin
  }

  def findInspectedTriphasicMeasuringPoints(): String = {

    s"""(WITH TMP_KAIFA AS (
       |  select count(distinct dt1.punto_servicio) --DT1.PUNTO_SERVICIO, MED.FECHA_DESDE
       |  from dwhcbprd.dw_datos_punto_servicio@biprdha dt1
       |    LEFT JOIN DWHCBPRD.dw_dt_medidor@biprdha med on med.numero_placa = dt1.numero_placa
       |    where dt1.numero_placa like '070%' and TRIM(dt1.fases) = 'TRIFASICO'
       |      AND med.Fecha_Desde is not null
       |),
       |ext_insp as (
       |  select t.*, dense_rank() over (partition by id order by COMMIT_DTTM desc /*to obtain the most recent*/) date_rank
       |  from deptorecener.dt_extraction_uruguay t
       |  where to_date(fecha, 'dd/mm/yyyy') IN (select max(to_date(fecha, 'dd/mm/yyyy')) from deptorecener.dt_extraction_uruguay)
       |        AND t.FECHAI1 IS NOT NULL
       |        AND ROWNUM < 100
       |), tmp_pm as(
       |  SELECT DISTINCT ID, CODE
       |  FROM MEASURING_POINT@bdmdmprd MP
       |)
       |select DISTINCT PM.ID PUNTO_SERVICIO, T.FECHAI1
       |from tmp_pm pm left join ext_insp t on pm.CODE = T.ID
       |where fechai1 is not null
       |  and t.id in (
       |      SELECT PUNTO_SERVICIO FROM  TMP_KAIFA KF
       |      WHERE to_date(T.fechaI1, 'DD/MM/YYYY') >= KF.FECHA_DESDE
       |  ))""".stripMargin
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
