import threading
import time
from google.cloud import bigquery

client = bigquery.Client()

queries = [
    # Query 1
    "SELECT PULocationID, COUNT(*) AS TotalTrips, AVG(trip_time) AS AverageTime FROM `dba-final-project-gcp.without_vm_upload.HV_Freq_Table` GROUP BY PULocationID ORDER BY TotalTrips DESC",
    # Query 2
    "SELECT * FROM `dba-final-project-gcp.without_vm_upload.HV_Freq_Table` WHERE (tolls > 5 AND trip_miles > 10) OR (congestion_surcharge > 2 AND trip_time > 1800)",
    # Query 3
    "SELECT DATE(Pickup_datetime) AS PickupDate, COUNT(*) AS NumberOfTrips FROM `dba-final-project-gcp.without_vm_upload.HV_Freq_Table` WHERE EXTRACT(HOUR FROM Pickup_datetime) BETWEEN 8 AND 10 GROUP BY PickupDate ORDER BY NumberOfTrips DESC",
    # Query 4
    "SELECT Hvfhs_license_num, Pickup_datetime, RANK() OVER (PARTITION BY Hvfhs_license_num ORDER BY trip_time DESC) AS Rank FROM `dba-final-project-gcp.without_vm_upload.HV_Freq_Table` WHERE trip_time IS NOT NULL",
    # Query 5
    "SELECT t.Hvfhs_license_num, t.Pickup_datetime, t.DropOff_datetime, avg_data.AverageTripTime FROM `dba-final-project-gcp.without_vm_upload.HV_Freq_Table` t JOIN ( SELECT Hvfhs_license_num, AVG(trip_time) AS AverageTripTime FROM `dba-final-project-gcp.without_vm_upload.HV_Freq_Table` GROUP BY Hvfhs_license_num ) avg_data ON t.Hvfhs_license_num = avg_data.Hvfhs_license_num",
    # Query 6
    "SELECT DISTINCT driver_pay FROM `dba-final-project-gcp.without_vm_upload.HV_Freq_Table` WHERE Hvfhs_license_num IN ( SELECT Hvfhs_license_num FROM `dba-final-project-gcp.without_vm_upload.HV_Freq_Table` WHERE trip_miles > 20 )",
    # Query 7
    "SELECT SUBSTR(CAST(Pickup_datetime AS STRING), 1, 10) AS PickupDate, COUNT(*) AS NumTrips FROM `dba-final-project-gcp.without_vm_upload.HV_Freq_Table` GROUP BY PickupDate ORDER BY NumTrips DESC",
    # Query 8
    "SELECT Hvfhs_license_num, SUM(CASE WHEN trip_time > 1800 THEN 1 ELSE 0 END) AS LongTrips, SUM(CASE WHEN trip_time <= 1800 THEN 1 ELSE 0 END) AS ShortTrips FROM `dba-final-project-gcp.without_vm_upload.HV_Freq_Table` GROUP BY Hvfhs_license_num",
    # Query 9
    "SELECT a.Hvfhs_license_num, a.Pickup_datetime, b.DropOff_datetime FROM `dba-final-project-gcp.without_vm_upload.HV_Freq_Table` a JOIN `dba-final-project-gcp.without_vm_upload.HV_Freq_Table` b ON a.Hvfhs_license_num = b.Hvfhs_license_num WHERE a.PULocationID != b.DOLocationID",
    # Query 10
    "SELECT PULocationID, COUNT(*) AS TotalTrips, AVG(trip_time) AS AverageTripTime FROM `dba-final-project-gcp.without_vm_upload.HV_Freq_Table` GROUP BY PULocationID HAVING AVG(trip_time) > 600"
]

def benchmark_query(query):
    try:
        start_time = time.time()
        job_config = bigquery.QueryJobConfig(use_query_cache=False)
        query_job = client.query(query, job_config=job_config)
        results = query_job.result(timeout=180)  # Adjust the timeout as needed
        end_time = time.time()

        total_bytes_processed = query_job.total_bytes_processed
        total_bytes_billed = query_job.total_bytes_billed or 0
        cost_estimate = (total_bytes_billed / 1e12) * 5

        print(f"Query: Time taken: {end_time - start_time} seconds")
        print(f"Data processed (bytes): {total_bytes_processed}")
        print(f"Data billed (bytes): {total_bytes_billed}")
        print(f"Estimated cost (USD): {cost_estimate:.2f}\n")
    except Exception as e:
        print(f"Error executing query: {query}")
        print(f"Exception: {e}\n")

def run_query(query):
    benchmark_query(query)

threads = []
for query in queries:
    thread = threading.Thread(target=run_query, args=(query,))
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()
