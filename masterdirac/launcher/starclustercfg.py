import starcluster
import boto
import boto.ec2
from boto.s3.key import Key
import os, os.path
from datetime import datetime
import ConfigParser
from pynamodb.models import Model
from pynamodb.attributes import (UnicodeAttribute, UTCDateTimeAttribute,
        NumberAttribute, UnicodeSetAttribute, JSONAttribute)
from pynamodb.exceptions import DoesNotExist
import StringIO
from flask import render_template
from tcdiracweb import app
import sys
import select
import boto.sqs
from boto.sqs.message import Message
import json
import multiprocessing
import subprocess 
from collections import defaultdict
class SCConfigError(Exception):
    pass

class ANMaster(Model):
    table_name = 'aurea-nebula-master'
    master_name = UnicodeAttribute( hash_key=True )
    date_created = UTCDateTimeAttribute( range_key=True, default=datetime.utcnow() )
    aws_region = UnicodeAttribute()
    key_pairs = JSONAttribute(default={})
    key_location = UnicodeAttribute()

class ANWorkerConfig(Model):
    table_name='aurea-nebula-worker'
    master_name = UnicodeAttribute( hash_key=True )
    cluster_name = UnicodeAttribute( range_key=True )
    cluster_type = UnicodeAttribute(default='data')
    node_type = UnicodeAttribute(default='m1.xlarge')
    aws_region = UnicodeAttribute(default='')
    num_nodes = NumberAttribute(default=0)
    nodes = UnicodeSetAttribute(default=[])
    state = NumberAttribute(default=0)
    config = UnicodeAttribute(default='')
    startup_log = UnicodeAttribute(default='')
    startup_pid = UnicodeAttribute(default='')

class ANDataServer(ANServer):
    def _create_model(self, master_name, cluster_name, aws_region):
        sc_model = ANWorkerConfig( master_name, cluster_name )
        sc_model.region = aws_region
        sc_model.cluster_type='data'
        sc_model.node_type='m1.xlarge'
        sc_model.save()
        return sc_model

class ANGPUServer(ANServer):
    def _create_model(self, master_name, cluster_name, aws_region):
        sc_model = ANWorkerConfig( master_name, cluster_name )
        sc_model.region = aws_region
        sc_model.cluster_type='gpu'
        sc_model.node_type='cg1.4xlarge'
        sc_model.save()
        return sc_model


def run_sc( starcluster_bin, url, master_name,cluster_name ):
    adv_ser = ANServer(master_name, cluster_name, no_create=True)
    pid = multiprocessing.current_process()
    base_message = {'cluster_name': cluster_name, 'master_name': master_name, 'pid':str(pid) }
        
    sqs =boto.sqs.connect_to_region("us-east-1")
    q = sqs.create_queue('starcluster-results')
    if adv_ser.active:
        base_message['type'] = 'system'
        base_message['msg'] = 'Error: already active'
        q.write( Message(body=json.dumps(message)) )
        return

    sc_command = "%s -c %s/%s/%s start -c %s %s" %( os.path.expanduser(starcluster_bin), url,master_name, cluster_name, cluster_name, cluster_name)
    base_message['command'] = sc_command
    sc_p = subprocess.Popen( sc_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True )
    adv_ser.set_active()
    adv_ser.set_startup_pid(str(sc_p.pid))
    log_subprocess_messages( sc_p, q, base_message)
    adv_ser.set_ready()
    """
    stdout = []
    stderr = []
    errors = False
    ctr = 0
    while True:
        ctr += 1
        reads = [sc_p.stdout.fileno(), sc_p.stderr.fileno()]
        ret = select.select(reads, [], [])
        for fd in ret[0]:
            base_message['time'] = datetime.now().isoformat()
            base_message['count'] = ctr
            if fd == sc_p.stdout.fileno():
                read = sc_p.stdout.readline()
                base_message['type'] ='stdout'
                base_message['time'] = datetime.now().isoformat()
                base_message['msg'] = read.strip()
                q.write(Message(body=json.dumps(base_message)))
            if fd == sc_p.stderr.fileno():
                read = sc_p.stderr.readline()
                base_message['type'] ='stderr'
                base_message['msg'] = read.strip()
                q.write(Message(body=json.dumps(base_message)))
        if sc_p.poll() != None:
            base_message['type'] = 'system'
            base_message['msg'] = 'Complete'
            if errors:
                base_message['msg'] += ':Errors exist'

            q.write(Message(body=json.dumps(base_message)))
            break"""

def gpu_logserver_daemon(starcluster_bin, url, master_name, cluster_name, action='start'):
    valid_actions = ['start', 'stop', 'status']
    assert action in valid_actions, "%s is not a valid action for gpu" % action
    base_message = {'cluster_name': cluster_name, 'master_name': master_name, 
            'action': action, 'component': 'gpu-logserver-daemon' }
    sqs =boto.sqs.connect_to_region("us-east-1")
    q = sqs.create_queue('starcluster-results')
    sc_command = "%s -c %s/%s/%s sshmaster -u sgeadmin %s " %( os.path.expanduser(starcluster_bin), url,master_name, cluster_name, cluster_name)
    if action == 'start':
        sc_command += "'bash /home/sgeadmin/GPUDirac/scripts/logserver.sh start'"
    if action == 'status':
        sc_command += "'bash /home/sgeadmin/GPUDirac/scripts/logserver.sh status'" 
    if action == 'stop':
        sc_command += "'bash /home/sgeadmin/GPUDirac/scripts/logserver.sh stop'"
    base_message['command'] = sc_command
    sc_p = subprocess.Popen( sc_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True )
    log_subprocess_messages( sc_p, q, base_message)

def gpu_daemon( starcluster_bin, url, master_name, cluster_name, gpu_id=0, action='start'):
    valid_actions = ['start', 'stop', 'status']
    assert action in valid_actions, "%s is not a valid action for gpu" % action

    base_message = {'cluster_name': cluster_name, 'master_name': master_name, 
            'gpu_id':str(gpu_id), 'action':action, 'component':'gpuserver-daemon' }
        
    sqs =boto.sqs.connect_to_region("us-east-1")
    q = sqs.create_queue('starcluster-results')
    sc_command = "%s -c %s/%s/%s sshmaster -u sgeadmin %s " %( os.path.expanduser(starcluster_bin), url,master_name, cluster_name, cluster_name)
    if action == 'start':
        sc_command += "'bash /home/sgeadmin/GPUDirac/scripts/gpuserver%i.sh start'" % gpu_id
    if action == 'status':
        sc_command += "'bash /home/sgeadmin/GPUDirac/scripts/gpuserver%i.sh status'" % gpu_id
    if action == 'stop':
        sc_command += "'bash /home/sgeadmin/GPUDirac/scripts/gpuserver%i.sh stop'" % gpu_id
    base_message['command'] = sc_command
    sc_p = subprocess.Popen( sc_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True )
    log_subprocess_messages( sc_p, q, base_message)

def cluster_restart( starcluster_bin, url, master_name, cluster_name):
    base_message = {'cluster_name': cluster_name, 'master_name': master_name, 
            'component':'restart'} 
    sqs =boto.sqs.connect_to_region("us-east-1")
    q = sqs.create_queue('starcluster-results')
    sc_command = "%s -c %s/%s/%s restart %s " %( os.path.expanduser(starcluster_bin), url,master_name, cluster_name, cluster_name)
    base_message['command'] = sc_command
    sc_p = subprocess.Popen( sc_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True )
    log_subprocess_messages( sc_p, q, base_message)

def cluster_terminate( starcluster_bin, url, master_name, cluster_name):
    base_message = {'cluster_name': cluster_name, 'master_name': master_name, 
            'component':'restart'} 
    sqs =boto.sqs.connect_to_region("us-east-1")
    q = sqs.create_queue('starcluster-results')
    sc_command = "%s -c %s/%s/%s terminate -f -c %s " %( os.path.expanduser(starcluster_bin), url,master_name, cluster_name, cluster_name)
    base_message['command'] = sc_command
    sc_p = subprocess.Popen( sc_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True )
    log_subprocess_messages( sc_p, q, base_message)


def log_subprocess_messages( sc_p, q, base_message):
    stdout = []
    stderr = []
    errors = False
    ctr = 0
    while True:
        ctr += 1
        reads = [sc_p.stdout.fileno(), sc_p.stderr.fileno()]
        ret = select.select(reads, [], [])
        for fd in ret[0]:
            base_message['time'] = datetime.now().isoformat()
            base_message['count'] = ctr
            if fd == sc_p.stdout.fileno():
                read = sc_p.stdout.readline()
                base_message['type'] ='stdout'
                base_message['time'] = datetime.now().isoformat()
                base_message['msg'] = read.strip()
                q.write(Message(body=json.dumps(base_message)))
            if fd == sc_p.stderr.fileno():
                read = sc_p.stderr.readline()
                base_message['type'] ='stderr'
                base_message['msg'] = read.strip()
                q.write(Message(body=json.dumps(base_message)))
        if sc_p.poll() != None:
            #process exitted
            ctr += 1
            base_message['time'] = datetime.now().isoformat()
            base_message['count'] = ctr
            base_message['type'] = 'system'
            base_message['msg'] = 'Complete'
            if errors:
                base_message['msg'] += ':Errors exist'
            q.write(Message(body=json.dumps(base_message)))
            break

def log_sc_startup( ):
    sqs =boto.sqs.connect_to_region("us-east-1")
    q = sqs.create_queue('starcluster-results')
    msg = q.read(120)
    msg_comb = defaultdict(list)
    err = ''
    clusters = set()
    while msg:
        mymsg = json.loads(msg.get_body())
        q.delete_message( msg )
        mymsg_key = mymsg['cluster_name'] + '-' + mymsg['master_name']
        msg_comb[mymsg_key].append(mymsg)
        msg = q.read(120)
    for key, msg_list in msg_comb.iteritems():
        first = msg_list[0]
        msg_list.sort(key=lambda x: x['count'])
        clusters.add( first['cluster_name'] )
        adv_ser = ANServer(first['master_name'], first['cluster_name'], no_create=True)
        log = adv_ser.startup_log 
        for msg in msg_list:
            if 'time' not in msg:
                msg['time'] = datetime.now().isoformat()
            if msg['type'] == 'stdout':
                log += '[%s] %s\n' % (msg['time'], msg['msg'])
            if False and msg['type'] == 'stderr':
                if msg['msg'][:3] == '>>>':
                    log += msg['msg'] + '\n'
            if msg['type'] == 'system':
                if msg['msg'][:8] == 'Complete':
                    log += '=' * 60 + '\n'
                    log += '[%s] %s\n' % (msg['time'], msg['msg'])
                    adv_ser.set_ready()
        adv_ser.set_startup_log( log )

    return list(clusters) 

if __name__ == "__main__":
    if not ANMaster.exists():
        ANMaster.create_table( read_capacity_units=2, write_capacity_units=1, wait=True)
    if not ANWorkerConfig.exists():
        ANWorkerConfig.create_table( read_capacity_units=2, write_capacity_units=1, wait=True)
    print log_sc_startup()
    exit()
    ams = ANMasterServer()
    #print ams.get_key('us-east-1')
    print ams.configure_data_cluster()
    print ams.configure_gpu_cluster()
    ds = ams.unstarted_data_clusters
    for ads in ds:
        print ads.config


    ds = ams.unstarted_gpu_clusters
    for ads in ds:
        print ads.config

