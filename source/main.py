from helper import getInstanceId, createhealthCheckAlarm, deleteAlarm

def main():
    running_instances = getInstanceId()
    deleteAlarm(running_instances)

    if not running_instances:
        print("No running instances found")
    else:
        for inst in running_instances:
            createhealthCheckAlarm(inst, "StatusCheckFailed_Instance", f"HealthCheckFailed_Instance-{inst}")
            createhealthCheckAlarm(inst, "StatusCheckFailed_System", f"HealthCheckFailed_System-{inst}")


main()