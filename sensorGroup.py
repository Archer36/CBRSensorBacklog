#!/usr/bin/env python
#

import sys
from cbapi.response.models import SensorGroup, Site, Process
from cbapi.example_helpers import build_cli_parser, get_cb_response_object
from hurry.filesize import size
from collections import defaultdict
import logging

log = logging.getLogger(__name__)


def list_sensor_groups(cb, parser, args):
    if args.group_name:
        selected_groups = args.group_name.split(",")
        totals = defaultdict(dict)
        print("Sensor Group,Sensor Group Backlog Total,Sensor Group Sensor Count,Sensors with 0 Processes")
        for sg in selected_groups:
            g = cb.select(SensorGroup).where("name:{0}".format(sg)).first()
            grouptotal = 0
            sensortotal = 0
            noproctotal = 0
            for sensor in g.sensors:
                if sensor.status == "Online":
                    query = cb.select(Process).where("hostname:{0}".format(sensor.hostname)).first()
                    if query is None:
                        noproctotal += 1
                        totals[g.name][sensor.hostname] = "noproc"
                    sensortotal += 1
                    grouptotal += int(sensor.num_eventlog_bytes)
            print(g.name + "," + str(grouptotal) + "," + str(sensortotal) + "," + str(noproctotal))
            totals[g.name]["backlog"] = grouptotal
            totals[g.name]["count"] = sensortotal
            totals[g.name]["noproc"] = noproctotal
            for key in totals[g.name]:
                if key != "backlog":
                    if key != "count":
                        if key!= "noproc":
                            print(key)

        print(totals)
    else:
        totals = defaultdict(dict)
        print("Sensor Group,Sensor Group Backlog Total,Sensor Group Sensor Count,Sensors with 0 Processes")
        for g in cb.select(SensorGroup):
            grouptotal = 0
            sensortotal = 0
            noproctotal = 0
            for sensor in g.sensors:
                if sensor.status == "Online":
                    query = cb.select(Process).where("hostname:{0}".format(sensor.hostname)).first()
                    if query is None:
                        noproctotal += 1
                        totals[g.name][sensor.hostname] = "noproc"
                    sensortotal += 1
                    grouptotal += int(sensor.num_eventlog_bytes)
            print(g.name+","+str(grouptotal)+","+str(sensortotal)+","+str(noproctotal))
            totals[g.name]["backlog"] = grouptotal
            totals[g.name]["count"] = sensortotal
            totals[g.name]["noproc"] = noproctotal
            for key in totals[g.name]:
                if key != "backlog":
                    if key != "count":
                        print(key)

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

    list_command = commands.add_parser("list", help="List all configured sensor groups")
    list_command.add_argument("-n", "--name", action="store", help="Sensor group name", required=False,
                                      dest="group_name")

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
