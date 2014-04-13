from pynamodb.models import Model
from pynamodb.attributes import (UnicodeAttribute, UTCDateTimeAttribute,
        NumberAttribute, UnicodeSetAttribute, JSONAttribute, BooleanAttribute
        )
from datetime import datetime
import hashlib

class ANSystemDefaults:
    class Meta:
        table_name='aurea-nebula-system-defaults'
        region = 'us-east-1'
    setting_name = UnicodeAttribute( hash_key = True )
    component = UnicodeAttribute( range_key = True ) 
    settings = JSONAttribute(default={})

def get_system_defaults( setting_name = None, component=None ):
    scan_fltr = {}
    if setting_name is not None:
        scan_fltr['setting_name__eq'] = setting_name
    if component is not None:
        scan_fltr['component__eq'] = component


    if setting_name is None:
        results = []
        for item in ANSystemDefaults.scan(**scan_fltr):
            res['setting_name'] = item.setting_name
            res['component'] = item.component
            res['settings'] = dict( item.settings )
            results.append( res )
        return results
    else:
        result = {}
        try:
            item = ANSystemDefaults.get( setting_name )
            return dict(item.settings)
        except ANSystemDefaults.DoesNotExist as dne:
            return {}


if __name__ == "__main__":
    if not ANSystemDefaults.exists():
        ANSystemDefaults.create_table( read_capacity_units=2,
            write_capacity_units=1, wait=True)
    #set data-cluster-logging
    item = ANSystemDefaults('logging', 'Data Cluster')
    item.settings = {
            'logging_file':'/scratch/sgeadmin/logs/datadirac.log',
            'log_bucket':'ndp-adversary',
            'log_s3_name_format': 'logs/datadirac/%s-%s.log',
            'level':'WARNING',
            'boto_level':'ERROR',
            'std_out_level':'INFO'
            }
    item.save() 
    item = ANSystemDefaults('cluster_init', 'Data Cluster')
    item.settings = {'data_log_dir': '/scratch/sgeadmin/data_log',
                    'working_dir': '/scratch/sgeadmin/working',
                    'init-queue':'tcdirac-master'}

