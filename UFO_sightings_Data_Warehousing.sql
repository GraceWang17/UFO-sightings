/* DataBase 2 W18 Assignment 1
Created: March 1, 2018
This assignment builds a data warehouse using dimensional modeling.
The dataset is using contains over 80,000 reports of UFO sightings over the last centurey. The Kaggle link is: https://www.kaggle.com/NUFORC/ufo-sightings .

The column metadata has the following attributes:
dateTime, city, state, country, shape, duration(seconds), duration(hours/min), comments, date posted, latitude and longitude of sighting .
 */

set serveroutput on
variable n number
exec :n := dbms_utility.get_time
 

SET ECHO ON;
SET SERVEROUTPUT ON;
SET LINESIZE 600;
SET DEFINE OFF;
SET TRIMSPOOL ON
SET COLSEP '|'
column column_name format a20
SPOOL A1.log;

/*
**The table contains eight colums
*/
DROP TABLE Dim_Date CASCADE CONSTRAINTS PURGE;

CREATE TABLE Dim_Date
(
        DateID INTEGER NOT NULL,
        FullDate DATE,
        CalendarYear VARCHAR2(10),
        CalendarMonth VARCHAR2(10),
        Day_of_Week VARCHAR2(20),
        Season VARCHAR2(30),
        Zodiac_sign VARCHAR2(30),
        Quarter VARCHAR2(20),
        CONSTRAINT dim_date_pk PRIMARY KEY (DateID)
);
/

/*
** create sequence for generating DateID
*/
DROP SEQUENCE Dim_Date_sequence;
CREATE SEQUENCE Dim_Date_sequence
       INCREMENT BY 1
       START WITH 1
       NOMAXVALUE
       NOCYCLE
       CACHE 20;


/*
**create a procedure to insert data into dim_date table;
*/
CREATE OR REPLACE PROCEDURE InputDateTime IS
        CURSOR dim_date_cursor IS
        SELECT DISTINCT SUBSTR(EVENTDT,1,INSTR(EVENTDT,' ',1)-1) FROM ufodb.ufo ;

        StringDate VARCHAR2(250);
        fDate DATE;
        CalYear VARCHAR2(10);
        fMonth VARCHAR2(10);
        fYear VARCHAR2(10);
		CalMonth VARCHAR2(10);
		dayOfWeek VARCHAR2(20);
		seasonFind VARCHAR2(10);
		theDay VARCHAR2(10);
		findSign VARCHAR2(30);
		theMonth VARCHAR2(10);
		theQuarter VARCHAR2(20);

   BEGIN

          OPEN dim_date_cursor;

          LOOP
		  
          FETCH dim_date_cursor INTO StringDate;
		  
          EXIT WHEN dim_date_cursor%NOTFOUND;
		  
          fDate :=TO_DATE(StringDate,'mm/dd/yyyy');
		  CalYear := TO_CHAR(fDate,'yyyy');
		  CalMonth := TO_CHAR(fDate,'MONTH');
		  dayOfWeek := TO_CHAR(fDate,'DAY');
		  theMonth := EXTRACT(MONTH FROM fDate);
		  theDay :=EXTRACT(DAY FROM fDate);
		  
		  	CASE
			WHEN theMonth >=3 AND theMonth <5 THEN  seasonFind := 'Spring';
			WHEN theMonth >=6 AND theMonth <8 THEN  seasonFind := 'Summer';
			WHEN theMonth >=9 AND theMonth <12 THEN  seasonFind := 'Fall';
			WHEN theMonth >=1 AND theMonth <3 THEN  seasonFind := 'Winter';
			WHEN theMonth =12 THEN  seasonFind := 'Winter';
			ELSE seasonFind := 'ERROR';
			END CASE;
	
			CASE
			WHEN (theMonth = '3' AND theDay >= '21') OR (theMonth = '4' AND theMonth <= '19') THEN findSign:='Aries';
			WHEN (theDay >= '20' AND theMonth  = '4') OR (theDay <= '20' AND theMonth  = '5') THEN findSign:='Taurus';
			WHEN (theDay >= '21' AND theMonth  = '5') OR (theDay <= '21' AND theMonth  = '6') THEN findSign:='Gemini';
			WHEN (theDay >= '22' AND theMonth  = '6') OR (theDay <= '22' AND theMonth  = '7') THEN findSign:='Cancer';
			WHEN (theDay >= '23' AND theMonth  = '7') OR (theDay <= '22' AND theMonth  = '8') THEN findSign:='Leo';
			WHEN (theDay >= '23' AND theMonth  = '8') OR (theDay <= '22' AND theMonth  = '9') THEN findSign:='Virgo';
			WHEN (theDay >= '23' AND theMonth  = '9') OR (theDay <= '22' AND theMonth  = '10') THEN findSign:='Libra';
			WHEN (theDay >= '23' AND theMonth  = '10') OR (theDay <= '21' AND theMonth  = '11') THEN findSign:='Scorpio';
			WHEN (theDay >= '22' AND theMonth  = '11') OR (theDay <= '21' AND theMonth  = '12') THEN findSign:='Sagittarius';
			WHEN (theDay >= '22' AND theMonth  = '12') OR (theDay <= '19' AND theMonth  = '1') THEN findSign:='Capricorn';
			WHEN (theDay >= '20' AND theMonth  = '1') OR (theDay <= '18' AND theMonth  = '2') THEN findSign:='Aquarius';
			WHEN (theDay >= '19' AND theMonth  = '2') OR (theDay <= '20' AND theMonth  = '3') THEN findSign:='Pisces';
			ELSE findSign:= 'ERROR';
			END CASE; 	
			
			CASE
			WHEN theMonth >=1 AND theMonth <4 THEN  theQuarter := 'First quarter, Q1';
			WHEN theMonth >=4 AND theMonth <7 THEN  theQuarter:= 'Second quarter, Q2';
			WHEN theMonth >=7 AND theMonth <10 THEN  theQuarter := 'Third quarter, Q3';
			WHEN theMonth >=10 AND theMonth <=12 THEN  theQuarter := 'Fourth quarter, Q4';
			ELSE theQuarter := 'ERROR';
			END CASE;
	
		
			
		  
           INSERT INTO dim_date(DateID,FullDate,CalendarYear,CalendarMonth,Day_of_Week,Season,Zodiac_sign,Quarter)
            VALUES (Dim_Date_sequence.NEXTVAL,fDate,CalYear,CalMonth,dayOfWeek,seasonFind,findSign,theQuarter);

          END LOOP;
          COMMIT;
          CLOSE dim_date_cursor;
        END;
/
SHOW ERROR

/* 
this is the dimensions time table
it arranges the row taken from ufo database and reformats 5 character
it has different attribute for example

    TIMEID FULLTIME             TY ISDAY TIMEFRAME            TWTIME
---------- -------------------- -- ----- -------------------- --------------------
        90 17:11                PM Day   After Noon           05:11:00PM


 */

DROP TABLE Dim_Time CASCADE CONSTRAINTS PURGE;
CREATE TABLE Dim_Time
(
	TimeID INTEGER NOT NULL,
	FullTime VARCHAR2(20),
	TypeTime CHAR(2),
	IsDay VARCHAR2(5),
	TimeFrame VARCHAR2(20),
	TwTime VARCHAR2(20),
	CONSTRAINT Dim_Time_pk PRIMARY KEY (TimeID)
);
/
DROP SEQUENCE Dim_Time_sequence;
CREATE SEQUENCE Dim_Time_sequence
	INCREMENT BY 1
	START WITH 1
	NOMAXVALUE
	NOCYCLE
	CACHE 20;
	/
	
	
	CREATE OR REPLACE PROCEDURE inputTime IS
	
     CURSOR Dim_Time_cursor IS SELECT DISTINCT SUBSTR(EVENTDT, INSTR(EVENTDT,' ')+1,5) FROM ufodb.ufo;
	 
	    StringTime VARCHAR2(250);
		fTime VARCHAR2(20);
		DayNight VARCHAR2(20);
		APType CHAR(2);
		hourTime VARCHAR2(200);
		TFrame VARCHAR2(20);
		hTime VARCHAR2(250);
   BEGIN 
 
		
	  OPEN Dim_Time_cursor;
	  
	  LOOP 
	  FETCH Dim_Time_cursor INTO StringTime;
	 
	  
	  fTime := StringTime;
	  
	  IF fTime >= '06:00' AND fTime <='18:00'
	  THEN
	   DayNight :='Day';
	  ELSE 
	  DayNight :='Night';
	  END IF;
	  
	  
	  IF fTime >= '00:00' AND fTime <='12:00'
	  THEN
	  APType :='AM';
	  ELSE 
	  APType :='PM';
	  END IF;
	  
	  IF fTime='24:00'
	  THEN
	  hTime:='00:00';
	  ELSE
	  hTime:= fTime;
	  END IF;

	  
	  
		hourTime := TO_CHAR(TO_DATE(hTime, 'hh24:mi:ss'), 'hh:mi:ssAM');

	  CASE 
	  WHEN  fTime = '06;00' THEN TFrame := 'Dawn';
	  WHEN  fTime = '12;00' THEN TFrame := 'Noon';
	  WHEN  fTime = '18;00' THEN TFrame := 'Dusk';
	  WHEN  fTime = '00:00' THEN TFrame := 'Midnight';
	  WHEN fTime > '00:00' AND fTime < '06;00' THEN TFrame := 'Early Morning';
	  WHEN fTime > '06:00' AND fTime < '12;00' THEN TFrame := 'Morning';
	  WHEN fTime > '12:00' AND fTime < '18;00' THEN TFrame := 'After Noon';
	  WHEN fTime > '18:00' AND fTime < '24;00' THEN TFrame := 'Evening';
	  ELSE TFrame := 'XX';
	  END CASE;
		
	  EXIT WHEN Dim_Time_cursor%NOTFOUND;
	  INSERT INTO Dim_Time(TimeID,FullTime,TypeTime,IsDay,TimeFrame,TwTime)
	  VALUES (Dim_Time_sequence.NEXTVAL,fTime,APType,DayNight,TFrame,hourTime);
	 
	  END LOOP;
	  
	  COMMIT;
	  
	  CLOSE Dim_Time_cursor;
	  
	END;
	/
	SHOW ERROR



/*
* Create location dimention table, which includes location ID, city, state, country, continent
*
*/
DROP TABLE Dim_Location CASCADE CONSTRAINTS PURGE;
CREATE TABLE Dim_Location
(
	LocationID NUMBER NOT NULL,
	City VARCHAR2(100),
	StateName VARCHAR2(100),
	Country VARCHAR2(100),
	Continent VARCHAR2(100),
	CONSTRAINT DIM_LOCATION_pk PRIMARY KEY(LocationID)
);

/*
* Create sequence number as loction ID in location table
*/
DROP SEQUENCE Dim_Location_sequence;
CREATE SEQUENCE Dim_Location_sequence
	INCREMENT BY 1
	START WITH 1
	NOMAXVALUE
	NOCYCLE
	CACHE 20;

/*
* This table uses to insert ID, city, state, country, continent into location table.
*/
CREATE OR REPLACE PROCEDURE popLoction IS
/* distinct ST, problem with it*/
       	CURSOR location_cursor IS SELECT DISTINCT CITY, ST, COUNTRY FROM ufodb.ufo WHERE CITY IS NOT NULL AND ST IS NOT NULL AND COUNTRY IS NOT NULL ;
		/*, ST, COUNTRY*   SELECT DISTINCT CITY FROM ufodb.ufo WHERE CITY IS NOT NULL AND ST IS NOT NULL AND COUNTRY IS NOT NULL/; */
		city_cur VARCHAR2(100);
		state_cur VARCHAR2(100);
		country_cur VARCHAR2(100);
		couName VARCHAR2(100);
        cont_cur VARCHAR2(100);
BEGIN
	OPEN location_cursor;
	LOOP
		FETCH location_cursor INTO city_cur, state_cur, country_cur;
		
		CASE TRIM(country_cur)
		WHEN 'us' THEN couName := 'United Statese';
		WHEN 'ca' THEN couName := 'Canada';
		WHEN 'gb' THEN couName := 'Great Britain';
		WHEN 'au' THEN couName := 'Australia';
		WHEN 'de' THEN couName := 'Germany';
		ELSE couName := 'XX';
		END CASE;
		
		CASE TRIM(country_cur)
		WHEN 'us' THEN cont_cur := 'North America';
		WHEN 'ca' THEN cont_cur := 'North America';
		WHEN 'gb' THEN cont_cur := 'Europe';
		WHEN 'au' THEN cont_cur := 'Australia';
		WHEN 'de' THEN cont_cur := 'Europe';
		ELSE cont_cur := 'NOT KNOWN';
		END CASE;
		
		
		
		EXIT WHEN location_cursor%NOTFOUND;
		      
		INSERT INTO Dim_Location (LocationID, City, StateName, Country, Continent)
			VALUES (Dim_Location_sequence.NEXTVAL, TRIM(city_cur),UPPER(TRIM(state_cur)),couName, cont_cur);
	END LOOP;
	COMMIT;
	CLOSE location_cursor;
				      
END;
/
SHOW ERROR

/*
* Create duration dimention table
*
*/
DROP TABLE Dim_Duration CASCADE CONSTRAINTS PURGE;
/* Create duration table, which includes duration ID, seconds, minutes, hours, and duration types (short, medium, long).*/
CREATE TABLE Dim_Duration
(
	DurationID NUMBER NOT NULL,
	Duration_in_Sec VARCHAR2(100),
	Duration_in_Min VARCHAR2(100),
	Duration_in_Hour VARCHAR2(100),
	DurType VARCHAR2(100),
	CONSTRAINT DIM_DURATION_pk PRIMARY KEY(DurationID)
);

/*
* Display minutes and add into the Dim_Duration table
*/
CREATE OR REPLACE FUNCTION minutes (DURATIONSEC IN CHAR)
RETURN CHAR IS 
	mins NUMBER;

BEGIN
	mins := TO_NUMBER(DURATIONSEC)/60;
	RETURN TO_CHAR(mins);
END;
/

/*
* Display hours and add into the Dim_Duration table
*/
CREATE OR REPLACE FUNCTION hours (DURATIONSEC IN CHAR) 
RETURN CHAR 
IS value NUMBER;

BEGIN
	value := TO_NUMBER(DURATIONSEC)/3600;
	RETURN TO_CHAR(value);
END;
/

/*
* Add all Duration_Type into the Dim_Duration table
* If duration_sec > 1 day (86400 sec), then it is long type, else if duration_sec < 1 hour, 
* then it is short type. other duration is medium type.
*/
CREATE OR REPLACE FUNCTION durationType (DURATIONSEC IN CHAR) 
RETURN CHAR
IS value VARCHAR2(20);

BEGIN
	IF TO_NUMBER(DURATIONSEC) > 86400  THEN
		value := 'Long';
	ELSIF TO_NUMBER(DURATIONSEC) < 3600 THEN
		value := 'Short';
	ELSIF TO_NUMBER(DURATIONSEC) >= 3600 AND TO_NUMBER(DURATIONSEC) <= 86400 THEN
		value := 'Medium';
	END IF;
	RETURN value;
END;
/

/*
* Create sequence for duration ID column
*/
DROP SEQUENCE Dim_Duration_sequence;
CREATE SEQUENCE Dim_Duration_sequence
	INCREMENT BY 1
	START WITH 1
	NOMAXVALUE
	NOCYCLE
	CACHE 20;

/*
* Create procedure to insert data into duration table.
*/
CREATE OR REPLACE PROCEDURE popDuration IS
	CURSOR duration_cursor IS SELECT DISTINCT TRIM(durationsec) FROM ufodb.ufo WHERE TRIM(durationsec) IS NOT NULL GROUP BY TRIM(durationsec);
		sec_cur VARCHAR2(100);

BEGIN
	OPEN duration_cursor;
	
	LOOP
	FETCH duration_cursor INTO sec_cur;
	EXIT WHEN duration_cursor%NOTFOUND;
	
	INSERT INTO Dim_Duration (DurationID, Duration_in_Sec, Duration_in_Min, Duration_in_Hour, DurType)
		VALUES(Dim_Duration_sequence.NEXTVAL, regexp_replace(sec_cur,'[^0-9]', ''), minutes(regexp_replace(sec_cur,'[^0-9]', '')), hours(regexp_replace(sec_cur,'[^0-9]', '')),
			durationType(regexp_replace(sec_cur,'[^0-9]', '')));
		
	END LOOP;
	commit;
	CLOSE duration_cursor;
END;
/
	
	
	
/*
* Create sequence for duration ID column
* it has field of name, has a type of not, and occurance
* it looks like this: 

   SHAPEID|NAME                                              |HASTYPE   |     OCCUR
----------|--------------------------------------------------|----------|----------
         9|light                                             |YES       |     16529

*
*/



DROP TABLE Dim_Shape CASCADE CONSTRAINTS PURGE;
CREATE TABLE Dim_Shape
(
	ShapeID NUMBER NOT NULL,
	Name VARCHAR2(50),
	HasType VARCHAR2(10),
	Occur NUMBER,
	CONSTRAINT Dim_Shape_pk PRIMARY KEY (ShapeID)
);
/

DROP SEQUENCE Dim_Shape_sequence;
CREATE SEQUENCE Dim_Shape_sequence
	INCREMENT BY 1
	START WITH 1
	NOMAXVALUE
	NOCYCLE
	CACHE 20;

CREATE OR REPLACE PROCEDURE popShape IS
	CURSOR shape_cursor IS SELECT DISTINCT SHAPE, COUNT(SHAPE) FROM ufodb.ufo WHERE SHAPE IS NOT NULL GROUP BY SHAPE;
		shape_cur VARCHAR2(100);
		num_cur VARCHAR2(100);
		sType VARCHAR2(10);
		numOcu NUMBER;
		ufoName VARCHAR2(100);
		
		
BEGIN
	OPEN shape_cursor;
	LOOP
	FETCH shape_cursor INTO shape_cur,num_cur;
	
	ufoName := TRIM(shape_cur); 
	
	
	CASE
	WHEN ufoName = 'other'   THEN sType := 'NO';
	WHEN ufoName = 'unknown' THEN sType := 'NO';
	ELSE  sType := 'YES';
	END CASE;
	
	numOcu := TO_NUMBER(num_cur);
	
	
	EXIT WHEN shape_cursor%NOTFOUND;
	
	INSERT INTO Dim_Shape (ShapeID, Name, HasType, Occur)
		VALUES(Dim_Shape_sequence.NEXTVAL, TRIM(shape_cur), sType, numOcu);
	END LOOP;
	COMMIT;
	CLOSE shape_cursor;
END;
/


EXEC InputDateTime();
EXEC inputTime();
EXEC popLoction();	
EXEC popDuration();
EXEC popShape();


/*
 * EVENTID is the primary Key
 * This is the UFO Sighting table, which is a fact table.
 * For each sighting, it records the DateID, TimeID, Duration, location,
 * Post date and season.
 * PK: Date, Location, Duration and Time
 * FK: DateID -> Dim_Date
 	   LocationID -> Dim_Location
 	   DurationID -> Dim_Duration
 	   TimeID -> Dim_Time
 */
DROP TABLE FACT_UFO CASCADE CONSTRAINTS PURGE;
CREATE TABLE FACT_UFO
(
    EventID NUMBER NOT NULL,
	DateID NUMBER NOT NULL,
    LocationID NUMBER NOT NULL,
    DurationID NUMBER NOT NULL,
    TimeID NUMBER NOT NULL,
	ShapeID NUMBER NOT NULL,
    Com VARCHAR2(300),
	Latitude VARCHAR2(100),
	Longitude VARCHAR2(100),
    CONSTRAINT ufo_pk PRIMARY KEY (EventID),
    CONSTRAINT date_fk FOREIGN KEY (DateID) REFERENCES Dim_Date(DateID),
    CONSTRAINT location_fk FOREIGN KEY (LocationID) REFERENCES Dim_Location(LocationID), 
	CONSTRAINT time_fk FOREIGN KEY (TimeID) REFERENCES Dim_Time(TimeID),
    CONSTRAINT duration_fk FOREIGN KEY (DurationID) REFERENCES Dim_Duration(DurationID),
	CONSTRAINT shape_fk FOREIGN KEY (ShapeID) REFERENCES Dim_Shape(ShapeID)
);

DROP SEQUENCE ufo_sequence;
CREATE SEQUENCE ufo_sequence
       INCREMENT BY 1
       START WITH 1
       NOMAXVALUE
       NOCYCLE
       CACHE 20;


CREATE OR REPLACE PROCEDURE popFact IS 
	
	date_cur VARCHAR2(250);
	city_cur VARCHAR2(100);
	st_cur VARCHAR2(100);
	country_cur VARCHAR2(100);
	dur_cur VARCHAR2(100);
	sha_cur VARCHAR2(100);
	comm VARCHAR2(300);
	lat VARCHAR2(100);
	Longi VARCHAR2(100);
	
	date_id NUMBER;
	loc_id NUMBER;
	Dur_id NUMBER;
	tim_id INTEGER;
	sha_id NUMBER;
	loc_city VARCHAR2(100);
	loc_st VARCHAR2(100);
	loc_country VARCHAR2(100);
	
	
	

 CURSOR fact_cursor IS SELECT DISTINCT EVENTDT, CITY,ST,COUNTRY, DURATIONSEC,SHAPE,COMMENTS , LAT, LONGI  FROM ufodb.ufo WHERE  CITY IS NOT NULL AND ST IS NOT NULL AND COUNTRY IS NOT NULL AND COMMENTS IS NOT NULL AND SHAPE IS NOT NULL;
 CURSOR loca_cursor IS SELECT DISTINCT dim_location.City, dim_location.StateName, dim_location.country,LocationID FROM Dim_Location Where Dim_Location.City = TRIM(city_cur) AND dim_location.StateName= UPPER(TRIM(st_cur));
CURSOR dur_cursor IS SELECT DISTINCT DurationID FROM Dim_Duration WHERE Dim_Duration.Duration_in_Sec = regexp_replace(dur_cur,'[^0-9]', '');
CURSOR tim_cursor  IS SELECT DISTINCT TimeID FROM Dim_Time WHERE Dim_Time.FullTime = TO_CHAR(SUBSTR(date_cur, INSTR(date_cur,' ')+1,5));
	CURSOR sha_cursor IS SELECT DISTINCT ShapeID FROM Dim_Shape WHERE Dim_Shape.Name = rtrim(sha_cur);
	CURSOR date_cursor  IS SELECT DISTINCT DateID FROM Dim_Date Where Dim_Date.FullDate =TO_DATE(SUBSTR(date_cur,1,INSTR(date_cur,' ',1)-1),'mm/dd/yyyy');
	
BEGIN
	OPEN fact_cursor;
	LOOP
		FETCH fact_cursor INTO date_cur, city_cur, st_cur,country_cur, dur_cur, sha_cur,comm, lat, Longi;
		EXIT WHEN fact_cursor%NOTFOUND;
			
	 	OPEN loca_cursor; 
		FETCH loca_cursor INTO loc_city, loc_st, loc_country, loc_id; 
 		CLOSE loca_cursor;
 
	    
		OPEN date_cursor;
		FETCH date_cursor INTO date_id;				
        CLOSE date_cursor;

	
        OPEN dur_cursor; 
 		FETCH dur_cursor INTO Dur_id;
 		CLOSE dur_cursor;
 
		
       OPEN tim_cursor;
		FETCH tim_cursor INTO tim_id;
		CLOSE tim_cursor;
		
		
		OPEN sha_cursor ;
		FETCH sha_cursor INTO sha_id;
		CLOSE sha_cursor;	


		INSERT INTO FACT_UFO (EventID,DateID, LocationID,DurationID, TimeID, ShapeID, Com, Latitude, Longitude)
		VALUES (ufo_sequence.NEXTVAL,date_id, loc_id,Dur_id, tim_id, sha_id,TRIM(comm), lat, Longi);
	
	END LOOP;
	commit;

	CLOSE fact_cursor;
END;
/
CALL popFact();


SPOOL OFF;
SET ECHO OFF;
exec :n := (dbms_utility.get_time - :n)/100;
exec dbms_output.put_line(:n);
SHOW ERROR
