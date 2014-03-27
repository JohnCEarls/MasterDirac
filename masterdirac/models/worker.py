from pynamodb.models import Model
from pynamodb.attributes import (UnicodeAttribute, UTCDateTimeAttribute,
        NumberAttribute, UnicodeSetAttribute, JSONAttribute)
from pynamodb.exceptions import DoesNotExist
from datetime import datetime

#STATES
OFF = 0
LAUNCHING = 0
UNINITIALIZED = 1
READY = 2
TERMINATED = 3

#worker defaults
IAM_PROFILE=

class ANWorker(Model):
    table_name='aurea-nebula-worker'
    master_name = UnicodeAttribute( hash_key=True )
    cluster_name = UnicodeAttribute( range_key=True )
    cluster_type = UnicodeAttribute(default='data')
    date_created = UTCDateTimeAttribute( default=datetime.utcnow() ) 
    aws_region = UnicodeAttribute(default='')
    num_nodes = NumberAttribute(default=0)
    nodes = UnicodeSetAttribute(default=[])
    state = NumberAttribute(default=0)
    starcluster_config = JSONAttribute(default={}) 
    startup_log = UnicodeAttribute(default='')
    startup_pid = UnicodeAttribute(default='')
    key = UnicodeAttribute(default = '')
    logging_config = JSONAttribute( default={} )
    cluster_init_config = JSONAttribute( default={} )


def new_data_server( cluster_prefix, aws_region, server_type, spot_bid):

class ANWorkerServer:
    def __init__(self, master_name, cluster_name, 
    

        """= {                                                                  
        'cluster_name':'dummy-cluster',                                         
        'aws_region':'us-east-1',                                               
        'key_name': 'somekey',                                                  
        'key_location': '/home/sgeadmin/somekey.key',                           
        'cluster_size': 1,                                                      
        'node_instance_type': 'm1.xlarge',                                      
        'node_image_id': 'ami-1234567',                                         
        'iam_profile':'some-profile',                                           
        'force_spot_master':True,                                               
        'spot_bid':2.00,                                                        
        'plugins':'p1,p2,p3'                                                    
    }""" 
if __name__ == "__main__":
    if not ANWorker.exists():
        ANWorker.create_table( read_capacity_units=2, 
            write_capacity_units=1, wait=True)




