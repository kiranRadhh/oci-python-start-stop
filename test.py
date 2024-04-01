import oci
import csv


# def list_all_instances(config, compartment_id, tag_key):
#     client = oci.core.ComputeClient(config)
#     try:

#         # instance_filter = {
#         #     "compartmentId": compartment_id,
#         #     "freeform_tags": {
#         #         tag_key: 'exists'
#         #     }
#         # }

#         list_instances_response = client.list_instances(compartment_id=compartment_id)
#         instances = list_instances_response.data
#         #print(instances)

#         tagged_instances = [instance for instance in instances if tag_key in instance.freeform_tags]
#         for instance in tagged_instances:
#             print("Instance ID:", instance.id)
#             print("Display Name:", instance.display_name)
#             print("Availability Domain:", instance.availability_domain)
#             print("Lifecycle State:", instance.lifecycle_state)
#             #print()

#             if instance.lifecycle_state != "RUNNING":
#                 start_instance(client, instance.id)
#                 print(f"Starting instance {instance.id}")

#     except oci.exceptions.ServiceError as e:
#         print("Error occurred:", e)

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



# def start_instance(client, instance_id):
#     try:
#         start_instance_response = client.instance_action(instance_id, "START")
#         print("Instance started successfully")
#     except oci.exceptions.ServiceError as e:
#         print(f"Error occurred while starting instance {instance_id}: {e}")

def main():
    # config = oci.config.from_file()
    # compartment_id = "ocid1.tenancy.oc1..aaaaaaaacy3gqq5abmumsk5322h4mum3a7744awaqjzqmpd3rdzjvekdpeja"
    # tag_key = "test-tag-one"

    schedule_table = get_schedule_table_from_csv('results.csv')
    print(schedule_table)

    #list_all_instances(config, compartment_id, tag_key)

if __name__ == "__main__":
    main()








