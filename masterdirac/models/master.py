from pynamodb.models import Model
from pynamodb.attributes import (UnicodeAttribute, UTCDateTimeAttribute,
        NumberAttribute, UnicodeSetAttribute, JSONAttribute)
from pynamodb.exceptions import DoesNotExist
from datetime import datetime

class ANMaster(Model):
    region = 'us-east-1'
    table_name = 'aurea-nebula-master'
    master_name = UnicodeAttribute( hash_key=True )
    date_created = UTCDateTimeAttribute( range_key=True, default=datetime.utcnow() )
    aws_region = UnicodeAttribute(default='')
    key_pairs = JSONAttribute(default={})
    instance_id = UnicodeAttribute(default='')
    working_dir = UnicodeAttribute(default='')
    comm_queue = UnicodeAttribute(default='')
    log_format = UnicodeAttribute(default='')
    log_server_config = JSONAttribute(default={})
    launcher_config = JSONAttribute(default={})

def get_default_master():
    """
    Returns empty list if default not found
    """
    return max( [item for item in ANMaster.scan(master_name__eq='default')], lambda x:x.date_created)

if __name__ == "__main__":
    if not ANMaster.exists():
        print "creaing aurea-nebula-master"
        ANMaster.create_table( read_capacity_units=2, 
            write_capacity_units=1, wait=True )
    try:
        default = get_default_master()
        if not default:
            raise ValueError("No default found")
        print default
        print "default exists"
    except ValueError as e:
        default = ANMaster( 'default' )
        default.working_dir = '/scratch/sgeadmin/master'
        default.log_server_config = {
            'external_server_name': '',
            'external_server_port':9021,
            'external_server_level':50,
            'internal_server_name':'localhost',
            'internal_server_port':9022,
            'internal_server_level':30,
            'stdout_level':10,
            'boto_level':40
            }
        default.launcher_config = {
            'launcher_sqs_in':'aurea-nebula-launcher-in',
            'launcher_sqs_out':'aurea-nebula-launcher-out',
            'launcher_config_bucket':'aurea-nebula',
            'gpu_dual_default_config':'aws-meta/cluster-defaults/gpu-dual.cfg',
            'data_default_config':'aws-meta/cluster-defaults/data.cfg',
            'key_location':'/home/sgeadmin',
            'local_config': '/scratch/sgeadmin'
            }
        default.save()
        print "created new default"

