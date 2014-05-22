from pynamodb.models import Model
from pynamodb.attributes import (UnicodeAttribute, UTCDateTimeAttribute,
        NumberAttribute, UnicodeSetAttribute, JSONAttribute, BooleanAttribute
        )
from datetime import datetime
import hashlib
import pprint
class ANSystemDefaults(Model):
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

    if setting_name is None or component is None:
        results = []
        for item in ANSystemDefaults.scan(**scan_fltr):
            res = {}
            res['setting_name'] = item.setting_name
            res['component'] = item.component
            res['settings'] = dict( item.settings )
            results.append( res )
        return results
    else:
        result = {}
        try:
            item = ANSystemDefaults.get( setting_name , component)
            result = dict(item.settings)
            return result
        except ANSystemDefaults.DoesNotExist as dne:
            return {}

def set_system_defaults():
    if not ANSystemDefaults.exists():
        ANSystemDefaults.create_table( read_capacity_units=2,
            write_capacity_units=1, wait=True)

    ######################
    ###DATA CLUSTER CONFIG
    ######################

    item = ANSystemDefaults('logging', 'Data Cluster')
    item.settings = {
            'logging_file':'/scratch/sgeadmin/logs/datadirac.log',
            'log_bucket':'datadirac-logging-aurea-nebula',
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
    item.save()

    ################
    ###MASTER CONFIG
    ################

    item = ANSystemDefaults('local_settings', 'Master')
    item.settings = {'working_dir': '/scratch/sgeadmin/master',
            'init-queue':'tcdirac-master',
            'branch':'master'
            }
    item.save()

    item = ANSystemDefaults('logging', 'Master')
    item.settings = {'log_format':'%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'external_server_name':'None',
            'external_server_port':9021,
            'external_server_level':50,
            'internal_server_name':'localhost',
            'internal_server_port':9022,
            'internal_server_level':30,
            'stdout_level':10,
            'boto_level':40}
    item.save()

    item = ANSystemDefaults('launcher_config', 'Master')
    item.settings = {'launcher_sqs_in':'aurea-nebula-launcher-in',
            'launcher_sqs_out':'aurea-nebula-launcher-out',
            'starcluster_bin' : '/home/sgeadmin/.local/bin/starcluster',
            'sc_config_url' : 'https://aurea-nebula.adversary.us/cm/config',
            'startup_logging_queue': 'sc-logging-queue',
            'key_location':'/home/sgeadmin'}
    item.save()

    item = ANSystemDefaults('loggingserver', 'Master')
    item.settings = {'directory':'/scratch/sgeadmin/logs',
        'log_filename':'None',
        'bucket':'aurea-nebula-logging',
        'log_format':'%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        'interval_type':'M',
        'interval':1,
        'port':9022   }
    item.save()

    ##################
    ###DUAL GPU CONFIG
    ##################

    item = ANSystemDefaults( 'directories', 'Dual GPU' )
    item.settings = {'source': '/scratch/sgeadmin/source',
            'results': '/scratch/sgeadmin/results',
            'log':'/scratch/sgeadmin/logs'
            }
    item.save()

    item = ANSystemDefaults( 'queues', 'Dual GPU')
    item.settings = {'init_q': 'tcdirac-master'}
    item.save()

    item = ANSystemDefaults('logging', 'Dual GPU')
    item.settings = {'log_format':'%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'external_server_name':'None',
            'external_server_port':9021,
            'external_server_level':50,
            'internal_server_name':'localhost',
            'internal_server_port':9022,
            'internal_server_level':30,
            'stdout_level':30,
            'boto_level':40}
    item.save()

    item = ANSystemDefaults('logserver', 'Dual GPU')
    item.settings = {'log_format':'%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'log':'/scratch/sgeadmin/logs',
            'directory': '/scratch/sgeadmin/logs',
            'log_filename':'None',
            'interval_type':'M',
            'interval':1,
            'port':9022,
            'bucket':'gpu-logging-aurea-nebula'
            }
    item.save()


if __name__ == "__main__":
    set_system_defaults()
    pprint.pprint( get_system_defaults() )
    pprint.pprint( get_system_defaults( component='Data Cluster') )
    pprint.pprint( get_system_defaults( setting_name = 'logging') )
    pprint.pprint( get_system_defaults( 'logging', 'Data Cluster') )
    pprint.pprint( get_system_defaults( component='Dual GPU') )
    import masterdirac.models.systemdefaults as sys_def
    directories = sys_def.get_system_defaults(component='Dual GPU',
            setting_name='directories')
    pprint.pprint(directories)
    q_cfg = sys_def.get_system_defaults(component='Dual GPU',
            setting_name='queues')
    init_q = q_cfg['init_q']
    print init_q
    import masterdirac.models.systemdefaults as sys_def
    log_settings = sys_def.get_system_defaults(component='Dual GPU',
            setting_name='logging')

    log_format = log_settings['log_format']
    es_name = log_settings['external_server_name']
    es_port = log_settings['external_server_port']
    es_level = log_settings['external_server_level']

    is_name = log_settings[ 'internal_server_name']
    is_port =  log_settings['internal_server_port']
    is_level = log_settings['internal_server_level']

    boto_level =  log_settings['boto_level']
    stdout_level = log_settings['stdout_level']
    pprint.pprint( log_settings )

    ls_settings = sys_def.get_system_defaults(component='Dual GPU',
                setting_name='logserver')
    pprint.pprint( ls_settings )
    log_dir = ls_settings['directory']
    LOG_FILENAME = ls_settings['log_filename']
    if LOG_FILENAME == 'None':
        md = {'instance-id': 'temp'} 
        LOG_FILENAME = md['instance-id'] + '.log'
    bucket = ls_settings[ 'bucket']
    interval_type = ls_settings[ 'interval_type' ]
    interval = int(ls_settings[ 'interval'])
    log_format = ls_settings[ 'log_format' ]
    port = ls_settings[ 'port' ]
