import boto
def delete(q_name):
    tail_pat = ['-command', '-response']
    for pat in tail_pat:
        if q_name[-1*len(pat):] == pat:
            return True
    head_pat = ['MasterDataNode-', 'from-data-to-gpu', 'from-gpu-to-agg']
    for pat in head_pat:
        if q_name[:len(pat)] == pat:
            return True
    if 'test' in q_name.split('-'):
        return True
    a = 'from-data-to-agg'
    b ='from-data-to-agg-all-q111-'
    if q_name[:len(a)] == a and q_name[:len(b)] != b:
        return True

    return False
sqs = boto.connect_sqs()
for queue in sqs.get_all_queues():
    if delete(queue.name):
        print "Deleting %s match pattern" % queue.name
        try:
            queue.delete()
        except:

            print "Error: Deleting %s match pattern" % queue.name
    elif queue.count() == 0 and queue.name[:4] == 'from':
        print "Not Deleting %s empty" % queue.name
        #queue.delete()
