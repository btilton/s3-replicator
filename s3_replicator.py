#!/usr/bin/env python
# -*- coding: utf-8 -*-

####################
#
# TOO MANY IMPORTS
#
####################

# import python built in libraries
import re
import sys
import daemon
import signal
import cPickle
import argparse
import traceback
import ConfigParser
from Queue import Queue
from os import remove, path
from datetime import datetime, timedelta
from threading import Thread, current_thread

# import 3rd party libraries
import boto3
from dateutil.tz import tzutc

# import local libraries
import utils.s3

###################
#
# ...FUNCTIONS...
#
###################


def main(one_off):
    while True:
        objects_cache = []
        with open('utils/s3_replicator_mcache.pickle', 'rb') as f:
            f.seek(0)
            while True:
                try:
                    objects_cache.append(cPickle.load(f))
                except EOFError:
                    break
        for section in config.sections():
            source_objects = parse_bucket_prefixes(section)
            to_sync = utils.s3.generate_objects_to_sync(source_objects,
                                                        objects_cache)
            print "s3_replicator: %s objects to sync" % len(to_sync)
            sys.stdout.flush()
            for obj in to_sync:
                q.put((section, obj))
            q.join()
            pq.join()
            # Upload cache to s3 if something new was added
            if len(to_sync) > 0:
                print "Uploading cache to s3"
                sys.stdout.flush()
                sourceclient.upload_file(
                    Bucket=backup_bucket,
                    Key='s3-replicator/s3_replicator_mcache.pickle',
                    Filename='utils/s3_replicator_mcache.pickle')
        if one_off:
            break


def sync_worker():
    while True:
        section, obj = q.get()
        sync_object(section,obj)
        q.task_done()


def pickle_worker():
    while True:
        obj = pq.get()
        with open('utils/s3_replicator_mcache.pickle', 'ab') as f:
            f.seek(0,2)
            cPickle.dump(obj, f)
        print "s3_replicator-{1}: {0} added to cache".format(
                                                    obj['Key'],
                                                    current_thread().getName())
        sys.stdout.flush()
        pq.task_done()


def sync_object(section,obj):
    """Download an object from one s3 account and upload it to another"""
    print "s3_replicator-{2}: downloading {0}/{1}".format(
                                            config.get(section,'source'),
                                            obj['Key'],
                                            current_thread().getName())
    sys.stdout.flush()
    try:
        sourceclient.download_file(Bucket=config.get(section,'source'),
                               Key=obj['Key'],
                               Filename=obj['Key'].split('/')[-1])
        print "s3_replicator-{2}: downloaded {0}/{1}".format(
                                            config.get(section,'source'),
                                            obj['Key'],
                                            current_thread().getName())
        sys.stdout.flush()
    except:
        print traceback.format_exc()
        print "s3_replicator-{2}: couldn't download {0}/{1}".format(
                                            config.get(section,'source'),
                                            obj['Key'],
                                            current_thread().getName())
        sys.stdout.flush()
        return
    print "s3_replicator-{2}: uploading {0}/{1}".format(
                                            config.get(section,'destination'),
                                            obj['Key'],
                                            current_thread().getName())
    sys.stdout.flush()
    upload_success = False
    while upload_success != True:
        try:
            print "s3_replicator-{0}: try upload".format(current_thread().getName())
            destclient.upload_file(Bucket=config.get(section,'destination'),
                                   Key=obj['Key'],
                                   Filename=obj['Key'].split('/')[-1])
            print "s3_replicator-{2}: uploaded {0}/{1}".format(
                                                config.get(section,'destination'),
                                                obj['Key'],
                                                current_thread().getName())
            sys.stdout.flush()
            upload_success = True
        except:
            print traceback.format_exc()
            print "s3_replicator-{2}: couldn't upload {0}/{1}".format(
                                                config.get(section,'destination'),
                                                obj['Key'],
                                                current_thread().getName())
            sys.stdout.flush()
    if upload_success:
        pq.put(obj)
    print "s3_replicator-{1}: cleaning up {0}".format(
                                            obj['Key'].split('/')[-1],
                                            current_thread().getName())
    sys.stdout.flush()
    try:
        remove(obj['Key'].split('/')[-1])
    except:
        print traceback.format_exc()
        pass


def parse_bucket_prefixes(section):
    """Generate list of objects to sync"""
    # Get include_prefixes list if present
    try:
        include_prefixes = config.get(section,'include_prefixes').split(',')
    except ConfigParser.NoOptionError:
        include_prefixes = None

    # Get exclude_prefixes list if present
    try:
        exclude_prefixes = config.get(section,'exclude_prefixes').split(',')
    except ConfigParser.NoOptionError:
        exclude_prefixes = None

    delta=int(config.get('DEFAULT','delta'))

    # Get objects_cache
    print "s3_replicator: Fetching objects list from source bucket %s" \
          % config.get(section,'source')
    sys.stdout.flush()
    source_objects = []
    if include_prefixes:
        for p in include_prefixes:
            for result in sourcepaginator.paginate(
                                        Bucket=config.get(section,'source'),
                                        Prefix=p):
                try:
                    for obj in utils.s3.shrink_objects_by_time(
                                                    result['Contents'],
                                                    timedelta(hours=delta)):
                        source_objects.append(obj)
                except KeyError:
                    pass
    else:
        for result in sourcepaginator.paginate(
                                        Bucket=config.get(section,'source')):
            for obj in utils.s3.shrink_objects_by_time(
                                                    result['Contents'],
                                                    timedelta(hours=delta)):
                source_objects.append(obj)
    print "s3_replicator: Generating list of objects that need to be synced"
    sys.stdout.flush()
    # Remove exclusions
    try:
        combined_excludes = "(^" + ")|(^".join(exclude_prefixes) + ")"
        source_objects = [obj for obj in source_objects \
                          if not re.match(combined_excludes,obj['Key'])]
    except TypeError:
        pass

    return source_objects


def reload_conf(signum, frame):
    print "Received Signal %s" % signum
    print "Reloading Config"
    config = ConfigParser.ConfigParser()
    config.read('utils/s3_replicator_buckets.ini')


####################
#
# THE INIT SECTION
#
####################

# config init
config = ConfigParser.ConfigParser()
config.read('utils/s3_replicator_buckets.ini')

# signal init
signal.signal(signal.SIGHUP, reload_conf)

# daemon init
context = daemon.DaemonContext()

# s3 client init
sourceclient = boto3.client('s3')
sourcepaginator = sourceclient.get_paginator('list_objects')
destclient = boto3.client('s3',
                aws_access_key_id=config.get('DEFAULT','dest_id'),
                aws_secret_access_key=config.get('DEFAULT','dest_secret'),
                region_name=config.get('DEFAULT','dest_region'))
destpaginator = destclient.get_paginator('list_objects')

# Queue init
q = Queue()
pq = Queue()

# Cache init
backup_bucket = 'my-bucket'
if not path.isfile('utils/s3_replicator_mcache.pickle'):
    try:
        sourceclient.download_file(Bucket=backup_bucket,
                               Key='s3-replicator/s3_replicator_mcache.pickle',
                               Filename='utils/s3_replicator_mcache.pickle')
    except:
        pass

# Threading init
# start pickle worker thread
pt = Thread(target=pickle_worker, name="thread_p")
pt.daemon = True
pt.start()
# start normal worker threads
NUM_THREADS = int(config.get('DEFAULT','num_threads'))
for i in range(NUM_THREADS):
    t = Thread(target=sync_worker, name="thread_"+str(i))
    t.daemon = True
    t.start()

##################
#
# MAIN IS GO!
#
##################

if __name__ == '__main__':
    description = """Replicate s3 buckets between accounts"""
    parser = argparse.ArgumentParser(
                        description=description,
                        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("-o", "--one-off", action='store_true',
                        help="Run through s3_replicator once")
    parser.add_argument("-d", "--daemon", action='store_true',
                        help="Run s3_replicator as a daemon")
    args = parser.parse_args()
    if args.daemon:
        with context:
            main(args.one_off)
    else:
        main(args.one_off)
