#!/usr/bin/env python
import ConfigParser
import masterdirac.server as master
import masterdirac.controller.serverinterface as si
import os
import boto
import shutil
import logging
import time
import boto.exception

def test_init_data( config ):
    logger = logging.getLogger("test_init_data")
    logger.info("*"*10)
    logger.info("*Starting*")
    logger.info("*"*10)
    result = master.init_data( config )
    logger.info("*"*8)
    logger.info("*PASSED*")
    logger.info("*"*8)
    return result

def test_get_work( config ):
    logger = logging.getLogger("test_get_work")
    logger.info("*"*10)
    logger.info("*Starting*")
    logger.info("*"*10)
    work = master.get_work( config )
    for job in work:
        logger.debug("%s, %i, %r, %i" % job)
        strain, permutations, shuffle, k = job
        assert len(strain) > 0
        assert permutations == config.getint('run_settings', 'permutations')
        assert k == config.getint('run_settings', 'k')
        assert shuffle 
    logger.info("*"*8)
    logger.info("*PASSED*")
    logger.info("*"*8)

def test_ServerManager_init(data_sizes, config ):
    logger = logging.getLogger("test_ServerManager_init")
    logger.info("*"*10)
    logger.info("*Starting*")
    logger.info("*"*10)
    work = master.get_work(config)
    manager = si.ServerManager(data_sizes, config )
    logger.info("ServerManager initialized")
    logger.info( "adding work")
    manager.add_work( work )
    assert manager.has_work()
    logger.info( "Manager has_work" )
    logger.info("*"*8)
    logger.info("*PASSED*")
    logger.info("*"*8)


def get_config( config_file ):
    cp =  ConfigParser.RawConfigParser( )
    cp.read(config_file)
    return cp

def clean_slate( cfg ):
    logger = logging.getLogger("clean_slate")
    try:
        shutil.rmtree( cfg.get('local_settings', 'working_dir') )
        logger.debug("Removed working dir")
    except OSError as e:
        logger.debug("No working directory")
    conn = boto.connect_s3()
    try:
        bkt = conn.get_bucket( cfg.get('dest_data', 'working_bucket' ))
        if bkt:
            for key in bkt.list():
                logger.debug("Deleting s3:/%s/%s" % ( 
                    cfg.get('dest_data', 'working_bucket' ), key.name ) )
                key.delete()
            conn.delete_bucket( cfg.get('dest_data', 'working_bucket') )
            logger.debug( "Deleting s3://%s" % \
                    cfg.get('dest_data', 'working_bucket') )
    except boto.exception.S3ResponseError as s3e:
        logger.exception("S3 exception on s3://%s" % \
                    cfg.get('dest_data', 'working_bucket'  ) )
    logger.debug( "Clean slate ... " )

if __name__ == "__main__":
    cfg = get_config('/home/sgeadmin/MasterDirac/config/server.cfg')
    FORMAT = cfg.get('logging', 'log_format')
    logging.basicConfig(format=FORMAT)
    logging.getLogger().setLevel(logging.DEBUG)
    logging.getLogger("boto").setLevel(logging.WARNING)
    #clean_slate( cfg )
    time.sleep(10)
    data_sizes = test_init_data( cfg )
    test_get_work( cfg )
    test_ServerManager_init( data_sizes, cfg )
