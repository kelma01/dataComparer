import json
import time
import psutil

process = psutil.Process()

start_time = time.time()

with open("datasets/data.json", "r") as file:
    json_data = json.load(file)

new_entry = {
    "sid": "row-test-test-tes3",
    "id": "00000000-0000-0000-C8E0-8E315E3AF06C",
    "position": 0,
    "created_at": 1712774929,
    "created_meta": "null",
    "meta": "{ }",
    "Unique_ID": "221806",
    "Indicator_ID": "386",
    "Name": "Ozone (O3)",
    "Measure": "Mean",
    "Measure_Info": "ppb",
    "Geo_Type_Name": "UHF34",
    "Geo_Join_ID": "103",
    "Geo_Place_Name": "Fordham - Bronx Pk",
    "Time_Period": "Summer 2014",
    "Start_Date": "2014-06-01T00:00:00",
    "Data_Value": "30.7",
    "Message": "null"
}

if isinstance(json_data, list):
    json_data.append(new_entry)
    with open("datasets/data.json", "w") as file:
        json.dump(json_data, file, indent=4)

end_time = time.time()

memory_usage = process.memory_info().rss / (1024 * 1024)  # MB cinsinden bellek kullanımı

print(f"========================================================================")
print(f"Writing Time of JSON formatted file: {end_time - start_time} seconds")
print(f"Memory Usage After Writing: {memory_usage:.2f} MB")
print(f"========================================================================")
#execute this line for running: `python .\writers\json_writer.py`