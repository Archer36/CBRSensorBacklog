from cbapi.response import *
from hurry.filesize import size
cb = CbResponseAPI()

total = 0

sensors = input('Enter list of hostnames separated by comma: ')

sensors_list = sensors.split(",")

for sensor in sensors_list:
    cur_sensor = "hostname:"+sensor.upper()
    cur_sensor_obj = cb.select(Sensor).where(cur_sensor).first()
    stats = cur_sensor_obj.queued_stats
    sensor_total = 0
    for i in stats:
        total += int(i['num_eventlog_bytes'])
        sensor_total += int(i['num_eventlog_bytes'])
    print(cur_sensor_obj.hostname)
    print(cur_sensor_obj.registration_time)
    print(sensor_total)
    print(total)
    print("--")

human_total = size(total)
print("Final Total: "+human_total+ " for "+str(len(sensors_list))+" sensors")
