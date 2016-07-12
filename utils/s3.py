#!/ursr/bin/env python
# reusable code for s3 operations

from datetime import datetime, timedelta
from dateutil.tz import tzutc

def shrink_objects_by_time(objects, delta):
    """Takes a list of objects output by boto3.bucket.objects.all and returns a list
    of objects newer than the timedelta object passed as delta. Times are UTC"""
    returnObjects = []
    now = datetime.now(tzutc())
    for o in objects:
        if (now - o['LastModified']) < delta:
            returnObjects.append(o)
    return returnObjects

def generate_objects_to_sync(source, destination):
    """Takes a list of objects from the source and a list of objects that exist in 
    the destination and returns any objects that are not in the destination"""
    returnObjects = []
    for o in source:
        if o not in destination:
            returnObjects.append(o)
    return returnObjects
