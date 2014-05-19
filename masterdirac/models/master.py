from pynamodb.models import Model
from pynamodb.attributes import (UnicodeAttribute, UTCDateTimeAttribute,
        NumberAttribute, UnicodeSetAttribute, JSONAttribute)
from datetime import datetime
import logging
import boto.ec2
import boto.exception
import masterdirac.models.systemdefaults as sys_def
#STATES
NA = -10
TERMINATED_WITH_ERROR = -5
INIT = 0
RUN = 10
TERMINATED = 30
logger = logging.getLogger(__name__)

class ANMaster(Model):
    class Meta:
        region = 'us-east-1'
        table_name = 'aurea-nebula-master'
    master_name = UnicodeAttribute( hash_key=True )
    date_created = UTCDateTimeAttribute(
            default=datetime.utcnow() )
    aws_region = UnicodeAttribute(default='')
    branch = UnicodeAttribute(default='master')
    key_pairs = JSONAttribute(default={})
    instance_id = UnicodeAttribute(default='')
    comm_queue = UnicodeAttribute(default='')
    status = NumberAttribute(default=0)

def to_dict( master_item ):
    result = {}
    result['master_name'] = master_item.master_name
    result['date_created'] = master_item.date_created
    result['aws_region'] = master_item.aws_region
    result['key_pairs'] = master_item.key_pairs
    result['instance_id'] = master_item.instance_id
    result['comm_queue'] = master_item.comm_queue
    result['branch'] = master_item.branch
    result['status'] = master_item.status
    return result

def get_active_master( branch=None ):
    masters = get_master()
    for master in masters:
        if branch is None or branch == master['branch']:
            if master['status'] in [INIT, RUN]:
                if confirm_master_active( master ):
                    return master
                else:
                    update_master( master['master_name'], 
                            status= TERMINATED_WITH_ERROR)
    return None

def handle_inconsistent_masters():
    masters = get_master()
    for master in masters:
        _handle_inconsistent_master( master)
        
def _handle_inconsistent_master( master):
    if master['status'] in [INIT, RUN] and not confirm_master_active(master):
        logger.error(("Master server with inconsistent status [%r]"
            ) % ( master ))
        try:
            update_master( master['master_name'], status= TERMINATED_WITH_ERROR)
            logger.error( ("Master server [%s] status changed"
                " to TERMINATED_WITH_ERROR") % ( master['master_name'] ) )
        except:
            logger.exception("Something horrible going on in the db")



def confirm_master_active(master):
    try:
        conn = boto.ec2.connect_to_region( master['aws_region'] )
        for reservation in conn.get_all_reservations( 
                                  instance_ids=[master['instance_id']] ):
            for inst in reservation.instances:
                if (inst.id, inst.state) == (master['instance_id'], 'running'):
                    return True
    except AttributeError as ae:
        logger.exception("Unable to connect to [%s] region." % (
            master['aws_region'] ))
        logger.error("Error attempting to confirm [%r]" % ( master ))
    except boto.exception.EC2ResponseError as ec2e:
        logger.exception("%s is an invalid instance id in %s region." % (
            master['instance_id'], master['aws_region'] ))
        logger.error("Error attempting to confirm [%r]" % ( master ))
    return False

def get_master( master_name=None):
    if master_name is None:
        results = []
        for item in ANMaster.scan():
            results.append( to_dict( item ) )
        return results
    else:
        try:
            print "in get_master"
            print master_name
            item = ANMaster.get( master_name )
            return to_dict( item )
        except ANMaster.DoesNotExist as dne:
            return {}

def update_master( master_name, 
        aws_region=None,
        key_pairs=None,
        instance_id = None,
        comm_queue = None,
        status = None,
        branch = None,
        **kwargs):

    item = ANMaster.get( master_name )
    if aws_region is not None:
        item.aws_region = aws_region
    if key_pairs is not None:
        item.key_pairs = key_pairs
    if instance_id is not None:
        item.instance_id = instance_id
    if comm_queue is not None:
        item.comm_queue = comm_queue
    if status is not None:
        item.status = status
    if branch is not None:
        item.branch = branch
    item.save()
    return to_dict( item )

def insert_master( master_name, 
        aws_region=None,
        key_pairs=None,
        instance_id = None,
        comm_queue = None,
        status = None,
        branch = None,
        **kwargs):

    item = ANMaster( master_name )
    if aws_region is not None:
        item.aws_region = aws_region
    if key_pairs is not None:
        item.key_pairs = key_pairs
    if instance_id is not None:
        item.instance_id = instance_id
    if comm_queue is not None:
        item.comm_queue = comm_queue
    if status is not None:
        item.status = status
    if branch is not None:
        item.branch = branch
    item.save()
    return to_dict( item )


if __name__ == "__main__":
    if not ANMaster.exists():
        print "creaing aurea-nebula-master"
        ANMaster.create_table( read_capacity_units=2, 
            write_capacity_units=1, wait=True )

