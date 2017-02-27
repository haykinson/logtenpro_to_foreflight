import sqlite3
import sys
from datetime import datetime, timedelta
import csv
import re

epoch = datetime(2000, 12, 31, 5, 0)

def debug(message):
	print >> sys.stderr, message

class LogTenFileHandler(object):
	def __init__(self, filename):
		self.filename = filename
		self.conn = sqlite3.connect(filename)
		self.conn.row_factory = sqlite3.Row

	def _query(self, querystring):
		c = self.conn.cursor()
		results = c.execute(querystring)
		for row in results:
			yield row

	def get_aircraft(self):
		q = """
SELECT 
	ZAIRCRAFT.ZAIRCRAFT_AIRCRAFTID AS AircraftID,
	ZAIRCRAFTTYPE.ZAIRCRAFTTYPE_TYPE AS TypeCode,
	ZAIRCRAFT.ZAIRCRAFT_YEAR AS Year,
	ZAIRCRAFTTYPE.ZAIRCRAFTTYPE_MAKE AS Make,
	ZAIRCRAFTTYPE.ZAIRCRAFTTYPE_MODEL AS Model,
	ZCCategory.ZLOGTENCUSTOMIZATIONPROPERTY_DEFAULTTITLE AS Category,
	ZCClass.ZLOGTENCUSTOMIZATIONPROPERTY_DEFAULTTITLE AS Class,
	CASE 
		WHEN ZAIRCRAFT.ZAIRCRAFT_UNDERCARRIAGEAMPHIB = 1 THEN 'AM'
		WHEN ZAIRCRAFT.ZAIRCRAFT_UNDERCARRIAGEFLOATS = 1 THEN 'Floats'
		WHEN ZAIRCRAFT.ZAIRCRAFT_UNDERCARRIAGESKIS = 1 THEN 'Skis'
		WHEN ZAIRCRAFT.ZAIRCRAFT_UNDERCARRIAGESKIDS = 1 THEN 'Skids'
		WHEN ZAIRCRAFT.ZAIRCRAFT_UNDERCARRIAGERETRACTABLE = 1 AND ZAIRCRAFT.ZAIRCRAFT_TAILWHEEL = 1 THEN 'RC'
		WHEN ZAIRCRAFT.ZAIRCRAFT_UNDERCARRIAGERETRACTABLE = 1 AND ZAIRCRAFT.ZAIRCRAFT_TAILWHEEL = 0 THEN 'RT'
		WHEN ZAIRCRAFT.ZAIRCRAFT_TAILWHEEL = 1 THEN 'FC'
		ELSE 'FT'
	END AS GearType,
	CASE ZCEngineType.ZLOGTENCUSTOMIZATIONPROPERTY_DEFAULTTITLE WHEN 'Reciprocating' THEN 'Piston' ELSE ZCEngineType.ZLOGTENCUSTOMIZATIONPROPERTY_DEFAULTTITLE END AS EngineType,
	CASE WHEN ZAIRCRAFT.ZAIRCRAFT_COMPLEX = 1 THEN 'x' ELSE NULL END AS Complex,
	CASE WHEN ZAIRCRAFT.ZAIRCRAFT_HIGHPERFORMANCE = 1 THEN 'x' ELSE NULL END AS HighPerformance,
	CASE WHEN ZAIRCRAFT.ZAIRCRAFT_PRESSURIZED = 1 THEN 'x' ELSE NULL END AS Pressurized
FROM
	ZAIRCRAFT
	LEFT OUTER JOIN ZAIRCRAFTTYPE ON ZAIRCRAFT.ZAIRCRAFT_AIRCRAFTTYPE = ZAIRCRAFTTYPE.Z_PK
	LEFT OUTER JOIN ZLOGTENCUSTOMIZATIONPROPERTY ZCCategory ON ZCCategory.Z_PK = ZAIRCRAFTTYPE.ZAIRCRAFTTYPE_CATEGORY
	LEFT OUTER JOIN ZLOGTENCUSTOMIZATIONPROPERTY ZCClass ON ZCClass.Z_PK = ZAIRCRAFTTYPE.ZAIRCRAFTTYPE_AIRCRAFTCLASS
	LEFT OUTER JOIN ZLOGTENCUSTOMIZATIONPROPERTY ZCEngineType ON ZCEngineType.Z_PK = ZAIRCRAFTTYPE.ZAIRCRAFTTYPE_ENGINETYPE
"""
		return self._query(q)

	def get_flights(self):
		q = """
SELECT
	ZFLIGHT.Z_PK AS ID,
	ZFLIGHT_FLIGHTDATE AS Date,
	ZAIRCRAFT.ZAIRCRAFT_AIRCRAFTID AS AircraftID,
	ZPFrom.ZPLACE_ICAOID AS [From],
	ZPTo.ZPLACE_ICAOID AS [To],
	NULL AS Route,
	NULL AS TimeOut,
	NULL AS TimeIn,
	NULL AS OnDuty,
	ZFLIGHT.ZFLIGHT_TOTALTIME / 60.0 AS TotalTime,
	ZFLIGHT.ZFLIGHT_PIC / 60.0 AS PIC,
	ZFLIGHT.ZFLIGHT_SIC / 60.0 AS SIC,
	ZFLIGHT.ZFLIGHT_NIGHT / 60.0 AS Night,
	ZFLIGHT.ZFLIGHT_SOLO / 60.0 AS Solo,
	ZFLIGHT.ZFLIGHT_CROSSCOUNTRY / 60.0 AS CrossCountry,
	ZFLIGHT.ZFLIGHT_DISTANCE AS Distance,
	ZFLIGHT.ZFLIGHT_DAYTAKEOFFS AS DayTakeoffs,
	ZFLIGHT.ZFLIGHT_DAYLANDINGS AS DayLandingsFullStop,
	ZFLIGHT.ZFLIGHT_NIGHTTAKEOFFS AS NightTakeoffs,
	ZFLIGHT.ZFLIGHT_NIGHTLANDINGS AS NightLandingsFullStop,
	ZFLIGHT.ZFLIGHT_TOTALLANDINGS AS AllLandings,
	ZFLIGHT.ZFLIGHT_ACTUALINSTRUMENT / 60.0 AS ActualInstrument,
	ZFLIGHT.ZFLIGHT_SIMULATEDINSTRUMENT / 60.0 AS SimulatedInstrument,
	ZFLIGHT.ZFLIGHT_HOBBSSTART AS HobbsStart,
	ZFLIGHT.ZFLIGHT_HOBBSSTOP AS HobbsEnd,
	ZFLIGHT.ZFLIGHT_TACHSTART AS TachStart,
	ZFLIGHT.ZFLIGHT_TACHSTOP AS TachEnd,
	NULL AS Holds,
	NULL AS Approach1,
	NULL AS Approach2,
	NULL AS Approach3,
	NULL AS Approach4,
	NULL AS Approach5,
	NULL AS Approach6,
	ZFLIGHT.ZFLIGHT_DUALGIVEN / 60.0 AS DualGiven,
	ZFLIGHT.ZFLIGHT_DUALRECEIVED / 60.0 AS DualReceived,
	ZFLIGHT.ZFLIGHT_SIMULATOR / 60.0 AS SimulatedFlight,
	ZFLIGHT.ZFLIGHT_GROUND / 60.0 AS GroundTraining,
	ZFLIGHT.ZFLIGHT_REMARKS AS PilotComments
FROM 
	ZFLIGHT
	LEFT OUTER JOIN ZAIRCRAFT ON ZAIRCRAFT.Z_PK = ZFLIGHT.ZFLIGHT_AIRCRAFT
	LEFT OUTER JOIN ZPLACE ZPFrom ON ZPFrom.Z_PK = ZFLIGHT.ZFLIGHT_FROMPLACE
	LEFT OUTER JOIN ZPLACE ZPTo ON ZPTo.Z_PK = ZFLIGHT.ZFLIGHT_TOPLACE
"""
		return self._query(q)

	def get_passengers(self):
		q = """
SELECT
	P.ZPERSON_NAME AS Name,
	ZFLIGHTCREW_FLIGHT AS FlightID,
	"PIC" AS Type
FROM
	ZFLIGHTCREW T
	INNER JOIN ZPERSON P ON P.Z_PK = T.ZFLIGHTCREW_PIC
WHERE
	T.ZFLIGHTCREW_PIC IS NOT NULL
	AND T.ZFLIGHTCREW_FLIGHT IS NOT NULL
	
UNION ALL

SELECT
	P.ZPERSON_NAME AS Name,
	ZFLIGHTCREW_FLIGHT AS FlightID,
	"INSTRUCTOR" AS Type
FROM
	ZFLIGHTCREW T
	INNER JOIN ZPERSON P ON P.Z_PK = T.ZFLIGHTCREW_INSTRUCTOR
WHERE
	T.ZFLIGHTCREW_INSTRUCTOR IS NOT NULL
	AND T.ZFLIGHTCREW_FLIGHT IS NOT NULL
	
UNION ALL

SELECT
	P.ZPERSON_NAME AS Name,
	ZFLIGHTPASSENGERS_FLIGHT AS FlightID,
	"Passenger" AS Type
FROM
	ZFLIGHTPASSENGERS T
	INNER JOIN ZPERSON P ON P.Z_PK = T.ZFLIGHTPASSENGERS_PAX1
WHERE
	T.ZFLIGHTPASSENGERS_PAX1 IS NOT NULL
	AND T.ZFLIGHTPASSENGERS_FLIGHT IS NOT NULL
	
UNION ALL

SELECT
	P.ZPERSON_NAME AS Name,
	ZFLIGHTPASSENGERS_FLIGHT AS FlightID,
	"Passenger" AS Type
FROM
	ZFLIGHTPASSENGERS T
	INNER JOIN ZPERSON P ON P.Z_PK = T.ZFLIGHTPASSENGERS_PAX2
WHERE
	T.ZFLIGHTPASSENGERS_PAX2 IS NOT NULL
	AND T.ZFLIGHTPASSENGERS_FLIGHT IS NOT NULL
"""
		return self._query(q)

class CsvFileHandler(object):
	def __init__(self, filename):
		self.filename = filename

	def write(self, aircraft, flights):
		csvfile = sys.stdout
		writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
		writer.writerow(['ForeFlight Logbook Import'] + [''] * 55)
		writer.writerow([''] * 56)
		writer.writerow(['Aircraft Table'] + [''] * 55)
		writer.writerow(['Text', 'Text', 'YYYY', 'Text', 'Text', 'Text', 'Text', 'Text', 'Text', 'Boolean', 'Boolean', 'Boolean'] + [''] * 44)
		aircraft_order = ['AircraftID', 'TypeCode', 'Year', 'Make', 'Model', 'Category', 'Class', 'GearType', 'EngineType', 'Complex', 'HighPerformance', 'Pressurized']
		writer.writerow(aircraft_order + [''] * 44)
		
		for a in aircraft:
			row = []
			for o in aircraft_order:
				if o in a:
					if a[o] is None:
						row.append('')
					else:
						row.append(a[o])
				else:
					row.append('')
			writer.writerow(row + [''] * 44)

		writer.writerow([''] * 56)
		writer.writerow(['Flights Table'] + [''] * 55)
		writer.writerow(['Date', 'Text', 'Text', 'Text', 'Text', 'hhmm', 'hhmm', 'hhmm', 'hhmm', 'Decimal', 'Decimal', 'Decimal', 'Decimal', 
			'Decimal', 'Decimal', 'Decimal', 'Number', 'Number', 'Number', 'Number', 'Number', 'Decimal', 'Decimal', 'Decimal', 'Decimal', 
			'Decimal', 'Decimal', 'Number', 'Packed Detail', 'Packed Detail', 'Packed Detail', 'Packed Detail', 'Packed Detail', 'Packed Detail', 
			'Decimal', 'Decimal', 'Decimal', 'Decimal', 'Text', 'Text', 'Packed Detail', 'Packed Detail', 'Packed Detail', 'Packed Detail', 
			'Packed Detail', 'Packed Detail', 'Boolean', 'Boolean', 'Boolean', 'Text', 'Decimal', 'Decimal', 'Number', 'Date', 'Boolean', 'Text'])
		flight_order = ['Date', 'AircraftID', 'From', 'To', 'Route', 'TimeOut', 'TimeIn', 'OnDuty', 'OffDuty', 'TotalTime', 'PIC', 'SIC', 'Night', 
			'Solo', 'CrossCountry', 'Distance', 'DayTakeoffs', 'DayLandingsFullStop', 'NightTakeoffs', 'NightLandingsFullStop', 'AllLandings', 
			'ActualInstrument', 'SimulatedInstrument', 'HobbsStart', 'HobbsEnd', 'TachStart', 'TachEnd', 'Holds', 'Approach1', 'Approach2', 
			'Approach3', 'Approach4', 'Approach5', 'Approach6', 'DualGiven', 'DualReceived', 'SimulatedFlight', 'GroundTraining', 'InstructorName', 
			'InstructorComments', 'Person1', 'Person2', 'Person3', 'Person4', 'Person5', 'Person6', 'FlightReview', 'Checkride', 'IPC', 
			'[Text]CustomFieldName', '[Numeric]CustomFieldName', '[Hours]CustomFieldName', '[Counter]CustomFieldName', '[Date]CustomFieldName', 
			'[Toggle]CustomFieldName', 'PilotComments']
		writer.writerow(flight_order)

		for f in flights:
			row = []
			for o in flight_order:
				if o in f:
					if f[o] is None:
						row.append('')
					else:
						row.append(f[o])
				else:
					row.append('')
			writer.writerow(row)


def apply_each(items, field, fcn):
	for item in items:
		item[field] = fcn(item[field]) if field in item else None

def standardize_make(field):
	if field is None:
		return None

	if field.startswith('CESSNA'):
		return 'Cessna'
	if field.startswith('PIPER'):
		return 'Piper'
	return field.split(' ')[0].strip()

def standardize_model(field):
	if field is None:
		return None
	model = field.split(',')[0].strip()
	return re.sub('[^A-Za-z0-9\-\'\s\/]', '', model)

def cleanup_notes(field):
	if field is None:
		return None
	return re.sub('[^A-Za-z0-9\-\',\.\!\?\s\/\:\&]', '', field)

def standardize_class(field):
	if field is None:
		return None
	if field == 'Single-Engine Land':
		return 'ASEL'

def convert_date(field):
	if field is None:
		return None

	days_since_epoch = int(field) / (60.0 * 60 * 24) 
	dt = epoch + timedelta(days=days_since_epoch)
	return dt.strftime('%Y-%m-%d')

def round_num(field):
	if field is None:
		return None

	f = float(field)
	return round(f, 1)

def main():
	if len(sys.argv) < 1:
		debug("please provide SQLite database filename as argument")
		sys.exit(1)

	filename = sys.argv[1]
	logten = LogTenFileHandler(filename)
	aircraft = [dict(x) for x in logten.get_aircraft()]
	flights = [dict(x) for x in logten.get_flights()]
	pax = [dict(x) for x in logten.get_passengers()]

	#aircraft
	apply_each(aircraft, 'Make', standardize_make)
	apply_each(aircraft, 'Model', standardize_model)
	apply_each(aircraft, 'Class', standardize_class)

	#fights
	apply_each(flights, 'Date', convert_date)
	apply_each(flights, 'Distance', round_num)
	apply_each(flights, 'PilotComments', cleanup_notes)
	for f in ['TachStart', 'TachEnd', 'HobbsStart', 'HobbsEnd']:
		apply_each(flights, f, round_num)

	#map passengers
	pax_by_flight = dict()
	for p in pax:
		_id = p['FlightID']
		if _id not in pax_by_flight:
			pax_by_flight[_id] = []
		pax_by_flight[_id].append(p)

	#get instructor and populate into flights
	for flight in flights:
		_id = flight['ID']
		if _id in pax_by_flight:
			for p in pax_by_flight[_id]:
				if p['Type'] == 'INSTRUCTOR':
					flight['InstructorName'] = p['Name']

	#populate passengers and pic
	for flight in flights:
		_id = flight['ID']
		if _id in pax_by_flight:
			psg = 1
			for p in pax_by_flight[_id]:
				if p['Type'] != 'INSTRUCTOR':
					flight['Person%d' % psg] = "%(Name)s;%(Type)s;" % p
					psg += 1

	c = CsvFileHandler('')
	c.write(aircraft, flights)

if __name__ == '__main__':
	main()
