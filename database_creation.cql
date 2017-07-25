DROP KEYSPACE IF EXISTS message_database;

CREATE KEYSPACE message_database WITH REPLICATION =  {
	'class': 'SimpleStrategy',
	'replication_factor': 1	
};

use message_database;


CREATE TABLE IF NOT EXISTS oru_messages (
	uid uuid, // id
	receivedTime bigint, // received at time
	sendTime bigint, // was sent at time
	msgctrlid text, // MSH-3
	patid int, // PV1-?, at first null/random, later on true patid?
	deviceID text, // null
	deviceType text,  // null
	textual map<text, text>, // was textual!
	alarm_code text, // contained in numeric as well
	bedInformation text, --check
	numeric map<text, frozen<tuple<float, text>>>,
	abnormal map<text, text>, --apparently not interesting
	visitNumber int, --apparently not interesting
	PRIMARY KEY (patid, sendTime, uid)
) with clustering order by (sendTime DESC);

CREATE TABLE IF NOT EXISTS adt_messages (
	uid uuid,
	receivedTime bigint,
	sendTime bigint,
	msgCtrlID text,
	PatID int,
	dob text,
	current boolean,
	bedInformation text,
	PRIMARY KEY (PatID, uid)
);

CREATE TABLE IF NOT EXISTS alarm_information (
	uid uuid,
	receivedTime bigint,
	sendTime bigint,
	msgCtrlID text,
	patid int,
	deviceid text,
	devicetype text,
	reason set<text>, // nope
	severness_counter map<text, int>, // nope
	PRIMARY KEY (PatID, uid)
);

CREATE TABLE IF NOT EXISTS alarm_messages (
	uid uuid,
	receivedTime bigint,
	sendTime bigint,
	msgCtrlID text,
	PatId int,
	deviceid text,
	devicetype text,
	ap_sf_code text, // mdil
	ap_sf_label text, // mdil message
	ap_ar_code text, // null
	ap_ar_label text, // null
	ap_threshold_direction text, // calc whether pos, neg, raising, sinking?
	ap_threshold_value float, // from list of boundaries
	ap_threshold_unit text, // from boundaries
	ap_causing_parameter_value float, // value that is out of bounds
	ap_causing_parameter_unit text, 
	ap_sf_level text, // alert message level
	ap_sf_alarm_code text, // alert message type
	PRIMARY KEY (PatId, uid)
);

CREATE TABLE IF NOT EXISTS patient_standard_values (
	receivedTime bigint,
	sendTime bigint,
	PatID int,
	parameters map<text, frozen <tuple<float,float>>>,
	current boolean,
	PRIMARY KEY (PatId, sendTime)
);

CREATE TABLE IF NOT EXISTS patient_bed_station (
	PatID int,
	roomNr text,
	bedInformation text,
	station text,
	time bigint,
	PRIMARY KEY (PatID, station)
);

CREATE TABLE IF NOT EXISTS error_log (
	msgctrlid text,
	errortype text,
	error_msg text,
	receivedTime bigint,
	sent_content text,
	sendTime bigint,
	PRIMARY KEY (msgctrlid)
);

CREATE TABLE IF NOT EXISTS station_name (
	stationID int,
	stationName text,
	PRIMARY KEY (stationID)
);