import oci
import csv
from datetime import datetime, timedelta

def list_all_instances(config, compartment_id, schedule_tag):
    client = oci.core.ComputeClient(config)
    try:
        list_instances_response = client.list_instances(compartment_id=compartment_id)
        instances = list_instances_response.data

        app_names = []
        tagged_instances = []

        for instance in instances:
            if instance.freeform_tags and schedule_tag in instance.freeform_tags:
                tagged_instances.append({
                    "Instance ID": instance.id,
                    "Display Name": instance.display_name,
                    "Availability Domain": instance.availability_domain,
                    "Lifecycle State": instance.lifecycle_state
                })

                app_name = instance.freeform_tags.get('appname', '')
                if app_name:
                    app_names.append(app_name)

        unique_app_names = list(set(app_names))

        return tagged_instances, unique_app_names

    except oci.exceptions.ServiceError as e:
        print("Error occurred:", e)

def get_schedule_table_from_csv(csv_file_path):
    schedule_table = {}
    
    with open(csv_file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            primary_key = row['shutdown_schedule_tag']
            start_time = row['start_time']
            stop_time = row['stop_time']
            offset = row['offset']
            
            schedule_table[primary_key] = {'start_time': start_time, 'stop_time': stop_time, 'offset': offset}
    
    return schedule_table

def get_offset(offset):
    offset_delta = timedelta(hours=int(offset))
    return offset_delta

def get_scheduled_time(schedule_table, ec2_shutdown_schedule_tag, offset):
    if ec2_shutdown_schedule_tag in schedule_table:
        start_time = datetime.strptime(schedule_table[ec2_shutdown_schedule_tag]['start_time'], "%H:%M")
        stop_time = datetime.strptime(schedule_table[ec2_shutdown_schedule_tag]['stop_time'], "%H:%M")
        return start_time + offset, stop_time + offset
    else:
        return None, None

def main():
    config = oci.config.from_file()
    compartment_id = "ocid1.tenancy.oc1..aaaaaaaacy3gqq5abmumsk5322h4mum3a7744awaqjzqmpd3rdzjvekdpeja"
    schedule_tag = "shutdown_schedule_tag"

    tagged_instances, unique_app_names = list_all_instances(config, compartment_id, schedule_tag)
    for instance in tagged_instances:
        print(instance)

    print("Unique Application Names:", unique_app_names)

    schedule_table = get_schedule_table_from_csv('results.csv')
    print(schedule_table)

    offset = get_offset('5')  # Example offset, change accordingly

    for instance in tagged_instances:
        ec2_shutdown_schedule_tag = instance.get('ec2_shutdown_schedule_tag')
        scheduled_start, scheduled_stop = get_scheduled_time(schedule_table, ec2_shutdown_schedule_tag, offset)
        if scheduled_start and scheduled_stop:
            print("Scheduled Start Time:", scheduled_start)
            print("Scheduled Stop Time:", scheduled_stop)
        else:
            print("Schedule data not found for instance:", instance['Instance ID'])

if __name__ == "__main__":
    main()
