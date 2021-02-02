import sys
import os
import argparse
import csv
from os.path import isdir, dirname
import urllib.request
from urllib.parse import parse_qs, urlparse

from obspy.core.stream import read

def read_mseed(url):
    return read(url, format="MSEED")

def download_waveform(rootdir, row):
    url = row['url']
    parsed_url = urlparse(url)
    qdic = parse_qs(parsed_url.query)
    seg_seed_id = "%s.%s.%s.%s" % \
                  (qdic['net'][0], qdic['sta'], qdic['loc'], qdic['cha'])

    # build name from event (dirty way):
    fname = "mag=%s;magt=%s;lat=%s;lon=%s;depth=%s;time=%s;id=%s" % \
            (row['evt_mag'], row['evt_magtype'], row['evt_lat'], row['evt_lon'],
             row['evt_depth_km'], row['evt_time'], row['Segment.db.id'])
    # build destpath:
    class_label = row['class']
    destfilepath = os.path.join(rootdir, class_label, seg_seed_id, fname + '.mseed')
    # save trace (no overwrite):
    if not os.path.isfile(destfilepath):
        stream = read_mseed(url)
        if len(stream) != 1:
            return
        trace = stream[0].detrent(type='linear')
        if trace.get_id() == seg_seed_id:
            trace.write(destfilepath, format="MSEED")

# PARSER:

parser = argparse.ArgumentParser()
parser.add_argument("destdir",
                    help="the destination directory of the downloaded files")
args = parser.parse_args()

destdir = args.destdir
if not isdir(dirname(destdir)):
    print('%s parent directory does not exist' % destdir)
    sys.exit(1)

print('Downloading and saving segment waveforms in:')
print(destdir)
print()

if not isdir(destdir):
    os.makedirs(destdir)

with open('eggs.csv', newline='') as csvfile:
    spamreader = csv.reader(csvfile)
    saved = 0
    for row in spamreader:
        try:
            download_waveform(destdir, row)
            saved += 1
        except Exception as exc:
            print('WARNING: Segment not saved (%s). URL: %s' % (str(exc), row['url']))
    print()
    print('Done, %d segment saved under %s' % (saved, ))



