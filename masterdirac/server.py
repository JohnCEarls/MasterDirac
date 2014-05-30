import boto.utils
import time
import logging

def get_master_name():
    """
    Returns a unique identifier for this server.
    TODO:
    This needs to be improved to handle
        -restart
        -the same instance popping up at a different time
        -multiple servers on same instance?
    """
    inst_id = boto.utils.get_instance_metadata()['instance-id']
    return inst_id

def main():
    from masterdirac.utils import debug
    import masterdirac.controller.servermanager as sm
    name = 'MasterServer'
    debug.initLogging()
    logger = logging.getLogger(name)
    logger.info( "Starting ServerManager" )
    manager = sm.ServerManager( get_master_name()  )
    terminate = False
    try:
        while not terminate:
            #handle any available runs
            manager.manage_run()
            #check for requests from web to manage servers
            manager.poll_launch_requests( timeout=2 )
            #check for requests from cluster to join the party
            manager.poll_for_server( timeout=2)
            #housekeeping, this is largely unimplemented
            terminate = manager.introspect()
        master_mdl.update_master( manager.master_model['master_name'], 
                status = master_mdl.TERMINATED )
    except:
        logger.exception("We be dead")
        raise
