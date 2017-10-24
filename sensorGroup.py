#!/usr/bin/env python
#

import sys
from cbapi.response.models import SensorGroup, Site, Process
from cbapi.example_helpers import build_cli_parser, get_cb_response_object
from hurry.filesize import size
from collections import defaultdict
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)



def list_sensor_groups(cb, parser, args):
    if args.group_name:
        selected_groups = args.group_name.split(",")
        log.info("Getting Data for: {}".format(selected_groups))

        totals = defaultdict(dict)
        sensor_stats = defaultdict(dict)
        print("Sensor Group,Sensor Group Backlog Total,Sensor Group Sensor Count,Sensors with 0 Processes")
        for sg in selected_groups:
            log.info("Looking at Sensor Group: {}".format(sg))
            g = cb.select(SensorGroup).where("name:{0}".format(sg)).first()

            if g is None:
                log.info("Sensor Group: {} is not valid".format(sg))
            else:
                grouptotal = 0
                sensortotal = 0
                noproctotal = 0

                for sensor in g.sensors:
                    if sensor.status == "Online":
                        log.info("Sensor {} is online".format(sensor.hostname))

                        if args.check_proc:
                            log.info("Checking for zero processes")

                            query = cb.select(Process).where("hostname:{0}".format(sensor.hostname)).first()
                            if query is None:
                                noproctotal += 1
                                totals[g.name][sensor.hostname] = "noproc"
                                sensor_stats[sensor.hostname]["noproc"] = "yes"

                        sensortotal += 1
                        grouptotal += int(sensor.num_eventlog_bytes)
                        sensor_stats[sensor.hostname]["backlog"] = int(sensor.num_eventlog_bytes)

                print("{},{},{},{}".format(g.name, grouptotal, sensortotal, noproctotal))

                totals[g.name]["backlog"] = grouptotal
                totals[g.name]["count"] = sensortotal
                totals[g.name]["noproc"] = noproctotal

                # for key in totals[g.name]:
                #     if key != "backlog":
                #         if key != "count":
                #             if key!= "noproc":
                #                 print(key)

                for key, value1 in sensor_stats.items():
                    if "noproc" in value1:
                        print(key+","+str(value1["backlog"])+","+value1["noproc"])

        print(totals)
        print(sensor_stats)
    else:
        log.info("Getting Data for: All Sensor Groups")
        totals = defaultdict(dict)
        print("Sensor Group,Sensor Group Backlog Total,Sensor Group Sensor Count,Sensors with 0 Processes")
        for g in cb.select(SensorGroup):
            log.info("Looking at Sensor Group: {}".format(g.name))

            grouptotal = 0
            sensortotal = 0
            noproctotal = 0

            for sensor in g.sensors:

                if sensor.status == "Online":
                    log.info("Sensor {} is online".format(sensor.hostname))

                    if args.check_proc:
                        log.info("Checking for zero processes")

                        query = cb.select(Process).where("hostname:{0}".format(sensor.hostname)).first()

                        if query is None:
                            log.info("Sensor {} has 0 processes!".format(sensor.hostname))
                            noproctotal += 1
                            totals[g.name][sensor.hostname] = "noproc"

                    sensortotal += 1
                    grouptotal += int(sensor.num_eventlog_bytes)

            print("{},{},{},{}".format(g.name,grouptotal,sensortotal,noproctotal))

            totals[g.name]["backlog"] = grouptotal
            totals[g.name]["count"] = sensortotal
            totals[g.name]["noproc"] = noproctotal

            # for key in totals[g.name]:
            #     if key != "backlog":
            #         if key != "count":
            #             print(key)

        print(totals)

def list_sensors(cb, parser, args):
    if args.group_name:
        group = cb.select(SensorGroup).where("name:{0}".format(args.group_name)).first()
    else:
        group = min(cb.select(SensorGroup), key=lambda x: x.id)

    for sensor in group.sensors:
        if sensor.status == "Online":
            query = cb.select(Process).where("hostname:{0}".format(sensor.hostname))
            print(sensor.hostname+","+str(len(query)))


def main():
    parser = build_cli_parser()
    commands = parser.add_subparsers(help="Sensor Group commands", dest="command_name")

    list_command = commands.add_parser("list", help="Check all or selected configured sensor groups")
    list_command.add_argument("-n", "--name", action="store", help="Sensor group name", required=False,
                                      dest="group_name")
    list_command.add_argument("-p", "--proc", action="store_true", help="Check for zero process hosts", required=False,
                              dest="check_proc")

    list_sensors_command = commands.add_parser("list-sensors", help="List all configured sensor groups")
    list_sensors_command.add_argument("-n", "--name", action="store", help="Sensor group name", required=False,
                                      dest="group_name")

    args = parser.parse_args()
    cb = get_cb_response_object(args)

    if args.command_name == "list":
        return list_sensor_groups(cb, parser, args)
    if args.command_name == "list-sensors":
        return list_sensors(cb, parser, args)


if __name__ == "__main__":
    sys.exit(main())
