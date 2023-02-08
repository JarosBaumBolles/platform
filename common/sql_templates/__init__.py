""" List of SQL templates to unify regeneration and scheduler load logic. """


def generate_openweather_update_sql_environment(project):
    return (
        f"UPDATE '{project}.standardized.measurements_environment'"
        " "
        "SET"
        " "
        "temperature=%s,"
        "temperature_feels_like=%s,"
        "humidity=%s,"
        "cloudiness=%s,"
        "wind_speed=%s,"
        "wind_direction=%s,"
        "dew_point=%s"
        " "
        "WHERE"
        " "
        "measurement_id=FARM_FINGERPRINT("
        "CONCAT("
        "FARM_FINGERPRINT(CONCAT(%s,%s)),"
        "%s,"
        "FARM_FINGERPRINT(%s)"
        ")"
        ");"
    )


def generate_openweather_update_sql_buildings(project):
    return (
        f"UPDATE '{project}.standardized.measurements_buildings'"
        " "
        "SET"
        " "
        "temperature=%s,"
        "humidity=%s,"
        "cloudiness=%s,"
        "wind_speed=%s,"
        "wind_direction=%s,"
        "dew_point=%s"
        " "
        "WHERE"
        " "
        "ref_hour_id=FARM_FINGERPRINT(CONCAT(%s,%s));"
    )


def generate_insert_sql_meters_data(project):
    return (
        f"INSERT INTO {project}.standardized_new.meters_data"
        "(ref_hour_id, ref_participant_id, ref_meter_id, data)"
        "VALUES (%s, %s, %s, %s)"
        ";"
    )


def generate_nantum_update_sql_buildings(project, measurement_type):
    return (
        f"UPDATE '{project}.standardized.measurements_buildings'"
        " "
        "SET"
        " "
        f"{measurement_type}=%s"
        " "
        "WHERE"
        " "
        "ref_hour_id=FARM_FINGERPRINT(CONCAT(%s,%s))"
        " "
        "AND"
        " "
        f"ref_building_id=%s"
        ";"
    )


def generate_ecostruxture_update_sql_buildings(project):
    return (
        f"UPDATE '{project}.standardized.measurements_buildings'"
        " "
        "SET"
        " "
        "electricity=%s"
        " "
        "WHERE"
        " "
        "ref_hour_id=FARM_FINGERPRINT(CONCAT(%s,%s))"
        " "
        "AND"
        " "
        f"ref_building_id=%s"
        ";"
    )


def generate_wattime_update_sql_environment(project, emissions_type):
    return (
        f"UPDATE '{project}.standardized.measurements_environment'"
        " "
        "SET"
        " "
        f"{emissions_type}=%s"
        " "
        "WHERE"
        " "
        "measurement_id=FARM_FINGERPRINT("
        "CONCAT("
        "FARM_FINGERPRINT(CONCAT(%s,%s)),"
        "%s,"
        "FARM_FINGERPRINT(%s)"
        ")"
        ");"
    )


def generate_wattime_update_sql_buildings(project, emissions_type):
    return (
        f"UPDATE '{project}.standardized.measurements_buildings'"
        " "
        "SET"
        " "
        f"{emissions_type}=%s"
        " "
        "WHERE"
        " "
        "ref_hour_id=FARM_FINGERPRINT(CONCAT(%s,%s));"
    )


QUERY_PORTAL_PROPERTIES_VIEW = """
WITH 
electra AS (
    SELECT DISTINCT meters.ref_hour_id as hour_id, assoc.ref_property_id as property_id, SUM(data * weight) as value 
    FROM production-epbp.standardized_new.meters_data as meters, production-epbp.standardized_new.meters_association as assoc, production-epbp.standardized_new.meters as meterType
    WHERE assoc.ref_meter_id = meters.ref_meter_id AND assoc.ref_meter_id = meterType.meter_id AND meterType.type = 'Electric'
    GROUP BY meters.ref_hour_id, assoc.ref_property_id
),
occupancy AS (
    SELECT DISTINCT meters.ref_hour_id as hour_id, assoc.ref_property_id as property_id, SUM(data * weight) as value 
    FROM production-epbp.standardized_new.meters_data as meters, production-epbp.standardized_new.meters_association as assoc, production-epbp.standardized_new.meters as meterType
    WHERE  assoc.ref_meter_id = meters.ref_meter_id AND assoc.ref_meter_id = meterType.meter_id AND meterType.type = 'Occupancy'
    GROUP BY meters.ref_hour_id, assoc.ref_property_id
),
emissions AS (
    SELECT DISTINCT meters.ref_hour_id as hour_id, assoc.ref_property_id as property_id, SUM(data * weight) as value 
    FROM production-epbp.standardized_new.meters_data as meters, production-epbp.standardized_new.meters_association as assoc, production-epbp.standardized_new.meters as meterType
    WHERE assoc.ref_meter_id = meters.ref_meter_id AND assoc.ref_meter_id = meterType.meter_id AND meterType.type = 'Average Grid Emissions'
    GROUP BY meters.ref_hour_id, assoc.ref_property_id
),
temper AS (
    SELECT DISTINCT meters.ref_hour_id as hour_id, assoc.ref_property_id as property_id, SUM(data * weight) as value 
    FROM production-epbp.standardized_new.meters_data as meters, production-epbp.standardized_new.meters_association as assoc, production-epbp.standardized_new.meters as meterType
    WHERE assoc.ref_meter_id = meters.ref_meter_id AND assoc.ref_meter_id = meterType.meter_id AND meterType.type = 'Ambient Temperature'
    GROUP BY meters.ref_hour_id, assoc.ref_property_id
)
SELECT DATETIME(cal.year, cal.month, cal.date, cal.hour, 0, 0) as timestamp, property_uri, electra.value AS energy, emissions.value AS emissions, occupancy.value AS occupancy, temper.value AS temperature, properties.footage AS footage
FROM electra, occupancy, emissions, temper, production-epbp.standardized_new.properties AS properties, production-epbp.standardized_new.calendar as cal
WHERE 
 cal.hour_id = electra.hour_id AND cal.hour_id = emissions.hour_id AND cal.hour_id = occupancy.hour_id AND cal.hour_id = temper.hour_id AND
 properties.property_id = electra.property_id AND properties.property_id = occupancy.property_id
GROUP BY  property_uri, energy, emissions, occupancy, temperature, footage, address, year, month, date, hour
ORDER BY timestamp asc
"""

QUERY_PORTAL_WEIGHTS_VIEW = """
SELECT property_uri, type, meter_uri, weight  FROM 
production-epbp.standardized_new.meters_data as meters, production-epbp.standardized_new.meters_association as assoc, production-epbp.standardized_new.meters as meterType, production-epbp.standardized_new.properties AS properties
WHERE assoc.ref_meter_id = meters.ref_meter_id AND assoc.ref_meter_id = meterType.meter_id AND properties.property_id = assoc.ref_property_id
GROUP BY type, property_uri, meter_uri, weight
ORDER BY type, property_uri
"""

QUERY_PRIVATE_PRESENTATION_VIEW = """
WITH 
electra AS (
    SELECT DISTINCT meters.ref_hour_id as hour_id, assoc.ref_property_id as property_id, SUM(data * weight) as value 
    FROM production-epbp.standardized_new.meters_data as meters, production-epbp.standardized_new.meters_association as assoc, production-epbp.standardized_new.meters as meterType
    WHERE assoc.ref_meter_id = meters.ref_meter_id AND assoc.ref_meter_id = meterType.meter_id AND meterType.type = 'Electric'
    GROUP BY meters.ref_hour_id, assoc.ref_property_id
),
occupancy AS (
    SELECT DISTINCT meters.ref_hour_id as hour_id, assoc.ref_property_id as property_id, SUM(data * weight) as value 
    FROM production-epbp.standardized_new.meters_data as meters, production-epbp.standardized_new.meters_association as assoc, production-epbp.standardized_new.meters as meterType
    WHERE  assoc.ref_meter_id = meters.ref_meter_id AND assoc.ref_meter_id = meterType.meter_id AND meterType.type = 'Occupancy'
    GROUP BY meters.ref_hour_id, assoc.ref_property_id
),
emissions AS (
    SELECT DISTINCT meters.ref_hour_id as hour_id, assoc.ref_property_id as property_id, SUM(data * weight) as value 
    FROM production-epbp.standardized_new.meters_data as meters, production-epbp.standardized_new.meters_association as assoc, production-epbp.standardized_new.meters as meterType
    WHERE assoc.ref_meter_id = meters.ref_meter_id AND assoc.ref_meter_id = meterType.meter_id AND meterType.type = 'Average Grid Emissions'
    GROUP BY meters.ref_hour_id, assoc.ref_property_id
),
temper AS (
    SELECT DISTINCT meters.ref_hour_id as hour_id, assoc.ref_property_id as property_id, SUM(data * weight) as value 
    FROM production-epbp.standardized_new.meters_data as meters, production-epbp.standardized_new.meters_association as assoc, production-epbp.standardized_new.meters as meterType
    WHERE assoc.ref_meter_id = meters.ref_meter_id AND assoc.ref_meter_id = meterType.meter_id AND meterType.type = 'Ambient Temperature'
    GROUP BY meters.ref_hour_id, assoc.ref_property_id
),
hourly_data AS (
    SELECT DATETIME(cal.year, cal.month, cal.date, cal.hour, 0, 0) as timestamp, cal.year, cal.month, cal.date, cal.hour, electra.property_id, electra.value AS energy, emissions.value AS emissions, occupancy.value AS occupancy, temper.value AS temperature, properties.footage AS footage, properties.address AS property_name
    FROM electra, occupancy, emissions, temper, production-epbp.standardized_new.properties AS properties, production-epbp.standardized_new.calendar as cal
    WHERE 
        cal.hour_id = electra.hour_id AND cal.hour_id = emissions.hour_id AND cal.hour_id = occupancy.hour_id AND cal.hour_id = temper.hour_id AND
        properties.property_id = electra.property_id AND properties.property_id = occupancy.property_id
    GROUP BY  property_id, energy, emissions, occupancy, temperature, footage, address, year, month, date, hour
    ORDER BY timestamp
),
avg_data AS (
    SELECT timestamp, year, month, date, hour, property_id, energy, AVG(emissions), AVG(occupancy), AVG(temperature), AVG(footage), 'Average' as property_name FROM hourly_data
    GROUP BY timestamp, property_id, energy, emissions, occupancy, temperature, footage, property_name, year, month, date, hour
    ORDER BY timestamp
)
SELECT * FROM hourly_data UNION ALL SELECT * from avg_data
"""

QUERY_PUBLIC_VIEW = """
SELECT 
timestamp as Time, 
year as Year, 
month as Month, 
date as Date, 
hour as Hour,
energy as Electricity,
emissions as Emissions,
occupancy as Occupancy,
temperature as Temperature, 
round(footage, -3) AS Footage, 
property_name AS Property
FROM production-epbp.standardized_new.private_presentation_view
"""

QUERY_PROPERTIES_1_VIEW = """
SELECT associations.ref_hour_id as hour_id, associations.ref_property_id as property_id, SUM(electra.data * associations.weight) AS energy, AVG(emissions.data * associations.weight) AS emissions, AVG(occupancy.data * associations.weight) AS occupancy, AVG(temperature.data * associations.weight) AS temperature  
FROM production-epbp.standardized_new.meters_association AS associations, production-epbp.standardized_new.meters_data AS electra, production-epbp.standardized_new.meters_data AS emissions, production-epbp.standardized_new.meters_data AS occupancy, production-epbp.standardized_new.meters_data AS temperature 
WHERE electra.ref_hour_id = associations.ref_hour_id AND associations.ref_meter_id = electra.ref_meter_id 
  AND emissions.ref_hour_id = associations.ref_hour_id AND associations.ref_meter_id = emissions.ref_meter_id
  AND occupancy.ref_hour_id = associations.ref_hour_id AND associations.ref_meter_id = occupancy.ref_meter_id
  AND temperature.ref_hour_id = associations.ref_hour_id AND associations.ref_meter_id = temperature.ref_meter_id
GROUP BY associations.ref_hour_id, associations.ref_property_id
"""

QUERY_PROPERTIES_2_VIEW = """
WITH 
electra AS (
    SELECT DISTINCT meters.ref_hour_id as hour_id, assoc.ref_property_id as property_id, SUM(data * weight) as value 
    FROM production-epbp.standardized_new.meters_data as meters, production-epbp.standardized_new.meters_association as assoc, production-epbp.standardized_new.meters as meterType
    WHERE assoc.ref_meter_id = meters.ref_meter_id AND assoc.ref_meter_id = meterType.meter_id AND meterType.type = 'Electric'
    GROUP BY meters.ref_hour_id, assoc.ref_property_id
),
occupancy AS (
    SELECT DISTINCT meters.ref_hour_id as hour_id, assoc.ref_property_id as property_id, SUM(data * weight) as value 
    FROM production-epbp.standardized_new.meters_data as meters, production-epbp.standardized_new.meters_association as assoc, production-epbp.standardized_new.meters as meterType
    WHERE  assoc.ref_meter_id = meters.ref_meter_id AND assoc.ref_meter_id = meterType.meter_id AND meterType.type = 'Occupancy'
    GROUP BY meters.ref_hour_id, assoc.ref_property_id
),
emissions AS (
    SELECT DISTINCT meters.ref_hour_id as hour_id, assoc.ref_property_id as property_id, SUM(data * weight) as value 
    FROM production-epbp.standardized_new.meters_data as meters, production-epbp.standardized_new.meters_association as assoc, production-epbp.standardized_new.meters as meterType
    WHERE assoc.ref_meter_id = meters.ref_meter_id AND assoc.ref_meter_id = meterType.meter_id AND meterType.type = 'Average Grid Emissions'
    GROUP BY meters.ref_hour_id, assoc.ref_property_id
),
temperature AS (
    SELECT DISTINCT meters.ref_hour_id as hour_id, assoc.ref_property_id as property_id, SUM(data * weight) as value 
    FROM production-epbp.standardized_new.meters_data as meters, production-epbp.standardized_new.meters_association as assoc, production-epbp.standardized_new.meters as meterType
    WHERE assoc.ref_hour_id = meters.ref_hour_id AND assoc.ref_meter_id = meters.ref_meter_id AND assoc.ref_meter_id = meterType.meter_id AND meterType.type = 'Ambient Temperature'
    GROUP BY meters.ref_hour_id, assoc.ref_property_id
)
SELECT electra.hour_id, cal.month, cal.date, cal.hour, electra.property_id, electra.value AS energy, emissions.value AS emissions, occupancy.value AS occupancy, properties.footage AS footage, properties.address AS address
FROM electra, occupancy, emissions, production-epbp.standardized_new.properties AS properties, production-epbp.standardized_new.calendar as cal
WHERE electra.hour_id = emissions.hour_id AND emissions.hour_id = occupancy.hour_id
 AND electra.property_id = emissions.property_id AND emissions.property_id = occupancy.property_id
 AND electra.property_id = properties.property_id AND cal.hour_id = electra.hour_id 
GROUP BY hour_id, property_id, energy, emissions, occupancy, footage, address, month, date, hour
ORDER BY hour_id
"""

