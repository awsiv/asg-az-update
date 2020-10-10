import boto3
from botocore.exceptions import ClientError
import json
import logging
import os
import re
import getopt
import sys

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# define boto3 clients
ec2_client = boto3.client('ec2')
asg_client = boto3.client('autoscaling')


def ignore_asg(asg_name, services):
    for service in services:
        service_pattern = '^{}-v[0-9]+$'.format(service)
        result = re.search(service_pattern, asg_name)
        if result:
            return False
    return True


def get_asgs():
    try:
        asg_describe = asg_client.describe_auto_scaling_groups()

        # Make sure the Auto Scaling group exists
        if len(asg_describe['AutoScalingGroups']) == 0:
            raise ValueError("Empty Auto Scaling Group")

        return asg_describe
    except ClientError as e:
        logging.error("Error retrieving Auto Scaling groups.")
        raise e


def get_azs_for_asg(asg):
    try:
        if 'AvailabilityZones' in asg.keys():
            return(asg['AvailabilityZones'])
        else:
            return(None)

    except ClientError as e:
        logging.error(
            "Error getting Availibility Zones for {}".format(asg['AutoScalingGroupName']))
        raise e


def get_subnets_for_asg(asg):
    try:
        if 'VPCZoneIdentifier' in asg.keys():
            logging.info("Current AZs+subnets for ASG '{}' = {}:{}".format(
                         asg['AutoScalingGroupName'], asg['AvailabilityZones'], asg['VPCZoneIdentifier']))
            return(asg['VPCZoneIdentifier'].split(','))
        else:
            return None

    except ClientError as e:
        logging.error(
            "Error getting Subnets (VPCZoneIdentifier) for {}".format(asg['AutoScalingGroupName']))
        raise e


def get_subnet_ids_for_az(az):
    if not az:
        return None
    try:
        subnet_describe = ec2_client.describe_subnets(
            Filters=[
                {
                    'Name': 'availabilityZone',
                    'Values': [az]
                },
            ],
        )

        # Make sure the Auto Scaling group exists
        if len(subnet_describe['Subnets']) == 0:
            raise ValueError("Empty subnets for {}".format(az))

        subnet_ids = []
        for subnets in subnet_describe['Subnets']:
            subnet_ids.append(subnets['SubnetId'])

        logging.debug("subnets {}: {}".format(az, subnet_ids))

        return (subnet_ids)

    except ClientError as e:
        logging.error(
            "Error getting subnets {}".format(az))
        raise e


def update_azs_for_asg(azs, subnets, asg):
    asg_name = asg['AutoScalingGroupName']

    max_size = asg['MaxSize']
    desired_capacity = asg['DesiredCapacity'] + 1
    if max_size < desired_capacity:
        max_size = desired_capacity

    try:
        response = asg_client.update_auto_scaling_group(
            AutoScalingGroupName=asg_name,
            AvailabilityZones=azs,
            VPCZoneIdentifier=subnets,
            # DesiredCapacity=desired_capacity,
            # MaxSize=max_size,
        )

    except ClientError as e:
        logging.error(
            "Error updating Availibility Zones for {}".format(asg_name))
        raise e


# TODO: revise this - its a bit tricky as this will stop all instances in all ASGs!
# WARNING: do not use this if you don't know what you are doing!
'''
def trigger_auto_scaling_instance_refresh(asg_name, strategy="Rolling",
                                          min_healthy_percentage=90, instance_warmup=300):

    try:
        response = asg_client.start_instance_refresh(
            AutoScalingGroupName=asg_name,
            Strategy=strategy,
            Preferences={
                'MinHealthyPercentage': min_healthy_percentage,
                'InstanceWarmup': instance_warmup
            })
        logging.info("Triggered Instance Refresh {} for Auto Scaling "
                     "group {}".format(response['InstanceRefreshId'], asg_name))

    except ClientError as e:
        logging.error("Unable to trigger Instance Refresh for "
                      "Auto Scaling group {}".format(asg_name))
        raise e
'''


def set_instances_unhealthy_for_azs(asg, azs):
    count = 0
    try:
        for instance in asg['Instances']:
            if instance['AvailabilityZone'] in azs:
                instance['HealthStatus'] = 'Unhealthy'
                logging.info(
                    "Setting instance {} unhealthy in {}({})".format(instance['InstanceId'], asg['AutoScalingGroupName'], instance['AvailabilityZone']))
                response = asg_client.set_instance_health(
                    InstanceId=instance['InstanceId'],
                    HealthStatus='Unhealthy',
                    ShouldRespectGracePeriod=False  # True
                )
                count += 1
    except ClientError as e:
        logging.error("Unable to update Instance health {}".format(e))
        raise e

    return count
    # def lambda_handler(event, context):


def update_az(az, az_list, operation):
    if not az:
        return az_list
    if not az_list:
        return None
    if az in az_list and operation == 'remove':
        az_list.remove(az)
    elif az not in az_list and operation == 'add':
        az_list.append(az)
    return az_list


def update_subnets(subnet_ids_to_update, subnet_id_list, operation):
    if not subnet_ids_to_update:
        return subnet_id_list
    if not subnet_id_list:
        return None

    for id in subnet_ids_to_update:
        if id in subnet_id_list and operation == 'remove':
            subnet_id_list.remove(id)
        elif id not in subnet_id_list and operation == 'add':
            subnet_id_list.append(id)
    return subnet_id_list


def ParseArgs(argv):
    options, args = getopt.getopt(
        argv[1:],
        's:b:w:d',
        ['services=', 'blacklist-az=', 'whitelist-az=', 'dryrun'])
    services = ""
    blacklist_az = ""
    whitelist_az = ""
    dryrun = False
    for option_key, option_value in options:
        if option_key in ('-s', '--services'):
            services = option_value
        elif option_key in ('-b', '--blacklist-az'):
            blacklist_az = option_value
        elif option_key in ('-w', '--whitelist-az'):
            blacklist_az = option_value
        elif option_key in ('-d', '--dryrun'):
            dryrun = True
    return services, blacklist_az, whitelist_az, dryrun, args


def main(argv):
    '''
    dry_run = os.environ['DRYRUN']
    blacklist_az = os.environ['BLACKLIST_AZ']
    whitelist_az = os.environ['WHITELIST_AZ']
    services = os.environ['SERVICE_NAMES']
    '''

    s, blacklist_az, whitelist_az, dryrun, args = ParseArgs(argv)
    services = s.split(",")

    # TODO: sanity check arguments
    logging.info("services = {}".format(services))
    logging.info("blacklist_az = {}".format(blacklist_az))
    logging.info("whitelist_az = {}".format(whitelist_az))
    logging.info("dryrun = {}".format(dryrun))

    # map azs with subnet ids
    blacklist_subnet_ids = get_subnet_ids_for_az(blacklist_az)
    whitelist_subnet_ids = get_subnet_ids_for_az(whitelist_az)

    logging.info("blacklist subnets ids = {}".format(blacklist_subnet_ids))
    logging.info("whitelist subnets ids = {}".format(whitelist_subnet_ids))

    asgs = get_asgs()

    for asg in asgs['AutoScalingGroups']:
        asg_name = asg['AutoScalingGroupName']

        if ignore_asg(asg_name, services):
            continue

        azs = get_azs_for_asg(asg)
        subnets = get_subnets_for_asg(asg)
        if not azs and not subnets:
            logging.error(
                "Auto Scaling group {} doesn't have AZ/subnet info".format(asg_name))
            continue

        logging.debug("current azs for {} = {}".format(asg_name, azs))

        update_az(blacklist_az, azs, 'remove')
        update_subnets(blacklist_subnet_ids, subnets, 'remove')
        update_az(whitelist_az, azs, 'add')
        update_subnets(whitelist_subnet_ids, subnets, 'add')

        # make sure that the az_list is non-empty
        if not azs or not subnets:
            logging.error(
                "Cannot blacklist AZ as it resulted in empty AZ list: {}".format(asg_name))
            continue

        logging.info("Updating asg: {} with {}({})".format(
            asg_name, azs, subnets))

        if dryrun == False:
            update_azs_for_asg(azs, ','.join(subnets), asg)
            instances = set_instances_unhealthy_for_azs(asg, blacklist_az)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main(sys.argv)
