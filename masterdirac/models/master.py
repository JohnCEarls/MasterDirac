from pynamodb.models import Model
from pynamodb.attributes import (UnicodeAttribute, UTCDateTimeAttribute,
        NumberAttribute, UnicodeSetAttribute, JSONAttribute)
from datetime import datetime

#STATES
OFF = 0
LAUNCHING = 0
UNINITIALIZED = 1
READY = 2
TERMINATED = 3


class ANMaster(Model):
    class Meta:
        region = 'us-east-1'
        table_name = 'aurea-nebula-master'
    master_name = UnicodeAttribute( hash_key=True )
    date_created = UTCDateTimeAttribute( range_key=True, default=datetime.utcnow() )
    aws_region = UnicodeAttribute(default='')
    key_pairs = JSONAttribute(default={})
    instance_id = UnicodeAttribute(default='')
    comm_queue = UnicodeAttribute(default='')

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
        default.save()
        print "created new default"

