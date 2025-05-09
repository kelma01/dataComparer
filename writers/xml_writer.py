import xml.etree.ElementTree as ET
import time
import psutil

start_time = time.time()
process = psutil.Process()

def escape_xml(value):
    if isinstance(value, str):
        return (
            value.replace("&", "&amp;")
                 .replace("<", "&lt;")
                 .replace(">", "&gt;")
                 .replace("'", "&apos;")
                 .replace('"', "&quot;")
        )
    return value

file_path = "datasets/data.xml"
tree = ET.parse(file_path)
root = tree.getroot()

new_entry = {
    "sid": "row-test-test-tes3",
    "id": "00000000-0000-0000-C8E0-8E315E3AF06C",
    "position": "0",
    "created_at": "1712774929",
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

new_element = ET.Element("item")
for key, value in new_entry.items():
    child = ET.SubElement(new_element, key)
    child.text = escape_xml(value)

root.append(new_element)

tree.write(file_path, encoding="utf-8", xml_declaration=True)

print(f"========================================================================")
print(f"Writing Time of XML formatted file: {time.time() - start_time} seconds")
print(f"Memory Usage After Writing: {process.memory_info().rss / (1024 * 1024):.2f} MB")
print(f"========================================================================")


#execute this line for running: `python .\writers\xml_writer.py`