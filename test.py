import oci
import csv
import datetime
import pytz


def list_all_instances(config, compartment_id, schedule_tag):
    compute_client = oci.core.ComputeClient(config)
    try:

        list_instances_response = compute_client.list_instances(compartment_id=compartment_id)
        instances = list_instances_response.data
        #print(instances)

        app_names = []
        tagged_instances = []

        #tagged_instances = [instance for instance in instances if tag_key in instance.freeform_tags]
        for instance in instances:
            if instance.freeform_tags and schedule_tag in instance.freeform_tags:
                tagged_instances.append({
                    "Instance ID": instance.id,
                    "Display Name": instance.display_name,
                    "Availability Domain": instance.availability_domain,
                    "Lifecycle State": instance.lifecycle_state,
                    "freeform_tags":instance.freeform_tags
                })

                app_name = instance.freeform_tags.get('appname', '')
                if app_name:
                    app_names.append(app_name)

        unique_app_names = list(set(app_names))

        return tagged_instances, unique_app_names, compute_client

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

def get_app_instance_table(tagged_instances, unique_app_name):

    app_instances = []
    web_server_instances = []
    app_server_instances = []
    database_server_instances = []
    sequence_check = ""   
    # for unique_app_name in unique_app_names:
        
        
        
    #print(tagged_instances)
    for instance in tagged_instances:
        print(instance)
        # ec2_tag_list = {}
        # for tags in instance[freeform_tags]:  
        
            
            
        ec2_tag_list = instance['freeform_tags']
            
            
        ec2_app_name_tag = ec2_tag_list['appname']
        ec2_cloud_function_tag = ec2_tag_list['cloud_function']
        ec2_shutdown_notification_tag = ec2_tag_list['Shutdown_Notification']
        ec2_shutdown_schedule_tag = ec2_tag_list['shutdown_schedule_tag']
        ec2_shutdown_off_hours_tag = ec2_tag_list['Off_Hours'] 
        ec2_sequence_type_tag = ec2_tag_list['sequence_type']   
        print(ec2_app_name_tag)
        print(unique_app_name)
        print(f'1: {ec2_shutdown_schedule_tag}')
        
        if ec2_app_name_tag == unique_app_name:
        
            if ec2_sequence_type_tag == 'sequence_number':
                cloud_function_values = {'app_instances': app_instances}
            
                #ec2_sequence_tag = ec2_tag_list['Sequence']
                ec2_sequence_start_tag = ec2_tag_list['Sequence_Start']
                ec2_sequence_stop_tag = ec2_tag_list['Sequence_Stop']
                
            
                ec2_table = {'Instance': instance, 'notification': ec2_shutdown_notification_tag, 'off_hours': ec2_shutdown_off_hours_tag,
                'sequence_start': ec2_sequence_start_tag, 'sequence_stop': ec2_sequence_stop_tag}
                
                app_instances.append(ec2_table)
                
                sequence_check = "True"

                print(f' test: {cloud_function_values}')
                
                
            elif ec2_sequence_type_tag == 'cloud_function':

                cloud_function_values = {'Web Server': web_server_instances, 'App Server': app_server_instances, 'Database Server': database_server_instances }
            
                ec2_table = {'Instance': instance, 'notification': ec2_shutdown_notification_tag, 'off_hours': ec2_shutdown_off_hours_tag}
                
                if ec2_cloud_function_tag == 'Web Server':
                
                    web_server_instances.append(ec2_table)
                    
                    
                elif ec2_cloud_function_tag == 'App Server':
                
                    app_server_instances.append(ec2_table)
                    
                    
                elif ec2_cloud_function_tag == 'Database Server':
                
                    database_server_instances.append(ec2_table)

                
                sequence_check = "False"
                
        else:
            print("Application does not match")
                
    print(f'2: {ec2_shutdown_schedule_tag}')            
    return sequence_check, cloud_function_values, ec2_shutdown_schedule_tag, unique_app_name       
    

    
# Compares the EC2 instance schedule tag with the offset table and provides start, stop times and offset
def process_offset_table(schedule_table, ec2_shutdown_schedule_tag):

    allowed_tag_values = list(schedule_table.keys())
    
    if ec2_shutdown_schedule_tag in allowed_tag_values:
        
        start_time = schedule_table[ec2_shutdown_schedule_tag]['start_time']
        stop_time = schedule_table[ec2_shutdown_schedule_tag]['stop_time']
        offset = schedule_table[ec2_shutdown_schedule_tag]['offset']

        print(f'{start_time}, {stop_time}, {offset}')
        
    else:
        print("The shutdown schedule tag is not in the allowed values")
    
    return start_time, stop_time, offset

# Calculate the time zone adjustment and day of the week
def get_offset(utc_offset):
    
    # Calculate the current UTC time which is the AWS time standard    
    utc_now=datetime.datetime.now()
    
    time_adjusted = datetime.datetime.now(pytz.timezone(utc_offset))
    day = time_adjusted.weekday()
    
    time_adjusted_year=time_adjusted.year
    time_adjusted_month=time_adjusted.month
    time_adjusted_day=time_adjusted.day
    time_adjusted_hour = time_adjusted.hour
    time_adjusted_minute = time_adjusted.minute
    time_adjusted_second = time_adjusted.second
    
    time_adjusted_formated = datetime.datetime(time_adjusted_year, time_adjusted_month, time_adjusted_day, time_adjusted_hour, time_adjusted_minute, time_adjusted_second)

    return time_adjusted_formated , day

def get_scheduled_time(ec2_shutdown_schedule_tag, start_time, stop_time, day_of_week):

    if 'weekdays' in ec2_shutdown_schedule_tag:
        if(day_of_week < 5):
                                
            scheduled_start= start_time
            scheduled_stop= stop_time
                   
        else:
            scheduled_start="false"
            scheduled_stop="false"
     
    elif 'daily' in ec2_shutdown_schedule_tag:
        
        if(day_of_week < 5):
            scheduled_start= start_time
            scheduled_stop= stop_time
            
        else:
            scheduled_start="false"
            scheduled_stop="false"
            
    elif 'business-hour' in ec2_shutdown_schedule_tag:
       
        if(day_of_week < 4):
            scheduled_start= start_time
            scheduled_stop="false"
            
        elif(day_of_week == 4):
            scheduled_start= start_time
            scheduled_stop= stop_time
          
        else:
            scheduled_start="false"
            scheduled_stop="false"

        
    else:
        scheduled_start="false"
        scheduled_stop="false"
        

    return scheduled_start, scheduled_stop
    

def get_scheduled_time_formated(time_adjusted, scheduled_start, scheduled_stop):

    if scheduled_start=='false' and scheduled_stop == 'false':
        pass
                   

    elif scheduled_start != 'false' and scheduled_stop == 'false':
                        
        time_adjusted_year=time_adjusted.year
        time_adjusted_month=time_adjusted.month
        time_adjusted_day=time_adjusted.day    
        
        scheduled_start_time_formated=datetime.datetime(time_adjusted_year, time_adjusted_month, time_adjusted_day,int(scheduled_start), 0, 0)
        scheduled_stop_time_formated=datetime.datetime.max
        
        
    elif scheduled_start == 'false' and scheduled_stop != 'false':
        
        time_adjusted_year=time_adjusted.year
        time_adjusted_month=time_adjusted.month
        time_adjusted_day=time_adjusted.day    
        
        scheduled_start_time_formated=datetime.datetime.min
        scheduled_stop_time_formated=datetime.datetime(time_adjusted_year, time_adjusted_month, time_adjusted_day,int(scheduled_stop), 0, 0)
        
    elif scheduled_start and scheduled_stop:
        
        time_adjusted_year=time_adjusted.year
        time_adjusted_month=time_adjusted.month
        time_adjusted_day=time_adjusted.day    
        
        scheduled_start_time_formated=datetime.datetime(time_adjusted_year, time_adjusted_month, time_adjusted_day,int(scheduled_start), 0, 0)
        scheduled_stop_time_formated=datetime.datetime(time_adjusted_year, time_adjusted_month, time_adjusted_day,int(scheduled_stop), 0, 0)
        
    else:
        print("There is no schedule action "+ scheduled_start + scheduled_stop)
    
    return scheduled_start_time_formated, scheduled_stop_time_formated

# Get EC2 action - start or stop based on scheduled_time and timezone
def get_ec2_action(scheduled_start_time, scheduled_stop_time, time_adjusted):

    if(time_adjusted > scheduled_start_time) and (time_adjusted < scheduled_stop_time):
        
        ec2_action="Start"
        
    else:
        
        ec2_action="Stop"

    return ec2_action
    

def validate_instance(sequence_check, ec2_action, cloud_function_values, unique_app_name, compute_client):


    if sequence_check == 'True':
    
        if ec2_action == 'Start':
        
            start_seq_response = start_seq_instances(cloud_function_values, unique_app_name, compute_client)
            
            return start_seq_response
            
        elif ec2_action == 'Stop':
        
            stop_seq_response = stop_seq_instances(cloud_function_values, unique_app_name, compute_client)
            return stop_seq_response
            
        else:
            seq_response = "Invalid EC2 action" 
            print(seq_response)
            return seq_response
    
    elif sequence_check == 'False':
    
        if ec2_action == 'Start':
        
            start_func_response = start_function_instances(cloud_function_values,unique_app_name, compute_client)
            return start_func_response
            
        elif ec2_action == 'Stop':
        
            stop_func_response = stop_function_instances(cloud_function_values,unique_app_name, compute_client)
            return stop_func_response
            
        else:
        
            print("Invalid EC2 action")
    
    else:
        func_response = "Invalid sequence check"
        print(func_response)
        return func_response
    
# Starts ec2 instances in a sequence
def start_seq_instances(cloud_function_values, unique_app_name, compute_client):

    appServers =  sorted(cloud_function_values['app_instances'], key=lambda x: x['sequence_start'], reverse=False)
    
    for server in appServers:
        instance = server['Instance']
        notification = server['notification']
        
        start_seq_instance_response = start_instance(instance, notification, compute_client)
    return start_seq_instance_response

# Stops ec2 instances in a sequence
def stop_seq_instances(cloud_function_values,unique_app_name, compute_client):

    
    appServers =  sorted(cloud_function_values['app_instances'], key=lambda x: x['sequence_stop'], reverse=False)
    
    for server in appServers:
        instance = server['Instance']
        notification = server['notification']
        off_hours = server['off_hours']
        
        if off_hours.lower() != 'yes':
            stop_seq_instance_response = stop_instance(instance, notification, compute_client)
        else:
            stop_seq_instance_response = "Off Hours set for instance:"+ instance['InstanceId']+". It will not be stopped"
            print(stop_seq_instance_response)
    
    return stop_seq_instance_response

# Starts ec2 instances part of a Cloud Function group
def start_function_instances(cloud_function_values, unique_app_name, compute_client):

    print(cloud_function_values)
    if 'Database Server' in cloud_function_values:
        DBServers = cloud_function_values['Database Server']
        
        for dbserver in DBServers:
            instance = dbserver['Instance']
            notification = dbserver['notification']
            
            start_func_instance_response = start_instance(instance, notification, compute_client)
        
    
    if 'App Server' in cloud_function_values:

        AppServers = cloud_function_values['App Server']
        
        for appserver in AppServers:
            instance = appserver['Instance']
            notification = appserver['notification']
            
            start_func_instance_response = start_instance(instance, notification, compute_client)
            
    WebServers = cloud_function_values['Web Server']
    
    for webserver in WebServers:
        instance = webserver['Instance']
        notification = webserver['notification']
        
        start_func_instance_response = start_instance(instance, notification, compute_client)
        

# Stops ec2 instances part of a Cloud Function group
def stop_function_instances(cloud_function_values, unique_app_name, compute_client):

    WebServers = cloud_function_values['Web Server']
    
    for webserver in WebServers:
        instance = webserver['Instance']
        notification = webserver['notification']
        off_hours = webserver['off_hours']

        if off_hours.lower() != 'yes':

        
            stop_func_instance_response = stop_instance(instance, notification, compute_client)
        
        else:

            Stop_Message_Off_Hours = "Off Hours set for instance:"+ instance['InstanceId']+". It will not be stopped"
            stop_func_instance_response = {'Instance-id':[instance['InstanceId']], 'Status': instance['State']['Name'], 'notification': notification, 'Comments': Stop_Message_Off_Hours, 'Date': datetime.datetime.now().isoformat() }
            #write_log(stop_func_instance_response)


    if 'App Server' in cloud_function_values:
    
        AppServers = cloud_function_values['App Server']
        
        for appserver in AppServers:
            instance = appserver['Instance']
            notification = appserver['notification']
            
            if off_hours.lower() != 'yes':

                print(f'test: {instance}')
    
                stop_func_instance_response = stop_instance(instance, notification, compute_client)
                
            else:
            
                Stop_Message_Off_Hours = f'Off-Hours is set for EC2 instance {instance["InstanceId"]}.Will not be stopped'
                stop_func_instance_response = {'Instance-id':instance['InstanceId'], 'Status': instance['State']['Name'], 'notification': notification, 'Comments': Stop_Message_Off_Hours, 'Date': datetime.datetime.now().isoformat() }
                #write_log(stop_func_instance_response)


    if 'Database Server' in cloud_function_values:
    
        DBServers = cloud_function_values['Database Server']
        
        for dbserver in DBServers:
            instance = dbserver['Instance']
            notification = dbserver['notification']
            off_hours = dbserver['off_hours']
            
            if off_hours.lower() != 'yes':
            
                stop_func_instance_response = stop_instance(instance, notification, compute_client)
                
            else:
            
                Stop_Message_Off_Hours = f'Off-Hours is set for EC2 instance {instance["InstanceId"]}.Will not be stopped'
                stop_func_instance_response = {'Instance-id':instance['InstanceId'], 'Status': instance['State']['Name'], 'notification': notification, 'Comments': Stop_Message_Off_Hours, 'Date': datetime.datetime.now().isoformat() }
                #write_log(stop_func_instance_response)
            
    
    return stop_func_instance_response

# The function to start the EC2 instance with a given instance-id

def start_instance(instance, notification, compute_client):
    instance_id = instance['Instance ID']

    
    if instance['Lifecycle State'] != "RUNNING":
        try:
            start_response = compute_client.instance_action(instance_id, "START")
            print(f"Starting instance {instance_id}")

        except oci.exceptions.ServiceError as e:            
            start_response = e
            raise e
        
    else:
        start_response = "Instance "+ instance_id+ "is already running. No action will be taken"
        print(start_response)
        #start_instance_response = {'Instance-id':[instance_id], 'Status': [instance['State']['Name']], 'notification': [notification], 'Comments': [start_response], 'Date': [datetime.datetime.now().isoformat()] }
        #write_log(start_instance_response)

    return start_response


# The function to stop the EC2 instance with a given instance-id

def stop_instance(instance, notification, compute_client):
    instance_id = instance['InstanceId']

    
    if instance.lifecycle_state != "STOPPED":
        try:
            stop_response = compute_client.instance_action(instance_id, "STOP")
            print(f"Stopping instance {instance_id}")
            
        except oci.exceptions.ServiceError as e:
            stop_response = e
            raise e
    
    
    else:
        stop_response = "Instance "+ instance_id+ "is already stopped. No action will be taken"
        print(stop_response)
        stop_instance_response = {'Instance-id':[instance_id], 'Status': [instance['State']['Name']], 'notification': [notification], 'Comments': [stop_response], 'Date': [datetime.datetime.now().isoformat()] }
        #write_log(stop_instance_response)

       
    return stop_response


# def start_instance(client, instance_id):
#     try:
#         start_instance_response = client.instance_action(instance_id, "START")
#         print("Instance started successfully")
#     except oci.exceptions.ServiceError as e:
#         print(f"Error occurred while starting instance {instance_id}: {e}")

def main():
    config = oci.config.from_file()
    compartment_id = ""
    schedule_tag = "shutdown_schedule_tag"


    tagged_instances, unique_app_names, compute_client = list_all_instances(config, compartment_id, schedule_tag)
    # for instance in tagged_instances:
    for unique_app_name in unique_app_names:
        if not tagged_instances:
            print("No instances in the region")
        else:
            #print(tagged_instances)
            schedule_table = get_schedule_table_from_csv('results.csv')
            sequence_check, cloud_function_values, ec2_shutdown_schedule_tag, unique_app_name = get_app_instance_table(tagged_instances, unique_app_name)
            start_time, stop_time, offset = process_offset_table(schedule_table, ec2_shutdown_schedule_tag)
            time_adjusted_formated, day = get_offset(offset)
            scheduled_start, scheduled_stop = get_scheduled_time(ec2_shutdown_schedule_tag, start_time, stop_time, day)
            scheduled_start_time_formated, scheduled_stop_time_formated = get_scheduled_time_formated(time_adjusted_formated, scheduled_start, scheduled_stop)
            ec2_action = get_ec2_action(scheduled_start_time_formated, scheduled_stop_time_formated, time_adjusted_formated)
            start_stop_action_response = validate_instance(sequence_check, ec2_action, cloud_function_values, unique_app_name, compute_client)
            
            print(f'{sequence_check}, {cloud_function_values}, {ec2_shutdown_schedule_tag}, {unique_app_name}')
            print(f'{time_adjusted_formated}, {day}')
            print(f'{scheduled_start}, {scheduled_stop}')
            print(f'{scheduled_start_time_formated}, {scheduled_stop_time_formated}')
        # Print the unique application names
        print("Unique Application Names:", unique_app_names)
        #print(schedule_table)

        #list_all_instances(config, compartment_id, tag_key)

if __name__ == "__main__":
    main()





