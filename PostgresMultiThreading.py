import time
import psycopg2


host = "35.196.84.94"
dbname = "database"
user = "admin"
password = "password"


queries = [
    """
    SELECT pulocationid, COUNT(*) AS totaltrips, AVG(trip_time) AS averagetime
    FROM hv_vehicle_trip
    GROUP BY pulocationid
    ORDER BY totaltrips DESC
    """,
    """
    SELECT *
    FROM hv_vehicle_trip
    WHERE (tolls > 5 AND trip_miles > 10) OR (congestion_surcharge > 2 AND trip_time > 1800)
    """,
    """
    SELECT SUBSTRING(pickup_datetime, 12, 2) AS PickupHour, COUNT(*) AS NumberOfTrips
    FROM hv_vehicle_trip
    WHERE SUBSTRING(pickup_datetime, 12, 2) BETWEEN '08' AND '10'
    GROUP BY PickupHour
    ORDER BY NumberOfTrips DESC


    """,
    """
    SELECT hvfhs_license_num, pickup_datetime, RANK() OVER (PARTITION BY hvfhs_license_num ORDER BY trip_time DESC) AS rank
    FROM hv_vehicle_trip
    WHERE trip_time IS NOT NULL
    """,
    """
    SELECT t.hvfhs_license_num, t.pickup_datetime, t.dropoff_datetime, avg_data.averagetriptime
    FROM hv_vehicle_trip t
    JOIN (
        SELECT hvfhs_license_num, AVG(trip_time) AS averagetriptime
        FROM hv_vehicle_trip
        GROUP BY hvfhs_license_num
    ) avg_data ON t.hvfhs_license_num = avg_data.hvfhs_license_num
    """,

    """
    SELECT DISTINCT driver_pay
FROM hv_vehicle_trip
WHERE hvfhs_license_num IN (
    SELECT hvfhs_license_num
    FROM hv_vehicle_trip
    WHERE trip_miles > 20
)

    """,
    """
   SELECT SUBSTRING(pickup_datetime FROM 1 FOR 10) AS PickupDate, COUNT(*) AS NumTrips
FROM hv_vehicle_trip
GROUP BY PickupDate
ORDER BY NumTrips DESC


   """,
    """
   SELECT hvfhs_license_num, 
         SUM(CASE WHEN trip_time > 1800 THEN 1 ELSE 0 END) AS LongTrips,
         SUM(CASE WHEN trip_time <= 1800 THEN 1 ELSE 0 END) AS ShortTrips
  FROM hv_vehicle_trip
  GROUP BY hvfhs_license_num
  
    """,
    """
    SELECT a.hvfhs_license_num, a.pickup_datetime, b.dropoff_datetime
  FROM hv_vehicle_trip a
  JOIN hv_vehicle_trip b ON a.hvfhs_license_num = b.hvfhs_license_num AND a.pulocationid != b.dolocationid
  LIMIT 10000
  
    """,
    """
   SELECT pulocationid, COUNT(*) AS TotalTrips, AVG(trip_time) AS AverageTripTime
  FROM hv_vehicle_trip
  GROUP BY pulocationid
  HAVING AVG(trip_time) > 600
  
  """
]



def benchmark_query(query, conn):
    start_time = time.time()
    with conn.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()
    end_time = time.time()
    print(f"Query:\n{query}\nTime taken: {end_time - start_time} seconds\n")



conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password)


try:
    for query in queries:
        benchmark_query(query, conn)
finally:
    conn.close()
