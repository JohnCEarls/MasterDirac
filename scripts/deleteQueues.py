import boto
def delete(q_name):
    tail_pat = ['-command', '-response']
    
    for pat in tail_pat:
        if q_name[-1*len(pat):] == pat:
            return True
    head_pat = ['MasterDataNode-']
    for pat in head_pat:
        if q_name[:len(pat)] == pat:
            return True
    if 'test' in q_name.split('-'):
        return False 
    return False
sqs = boto.connect_sqs()
for queue in sqs.get_all_queues():
    if delete(queue.name):
        print "Deleting %s match pattern" % queue.name
        queue.delete()
    elif queue.count() == 0:
        print "Deleting %s empty" % queue.name
        queue.delete()
