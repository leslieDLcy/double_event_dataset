import sys
import os
import argparse
import csv
from os.path import isdir, dirname, join, isfile  # abspath, basename,
# import urllib.request
from urllib.parse import parse_qs, urlparse

from obspy.core.stream import read


# script functions:


def download_waveform(rootdir, row):
    """Download a waveform from a row of the `urls.csv` file.

    :param rootdir: the base root directory. All files will be created therein
        inside subdirectories. Each subdirectory will be created (os.makedirs)
        if it does not exist
    :param rwo: dict representing a row of the `urls.csv` file
    """
    url = row['url']
    parsed_url = urlparse(url)
    qdic = parse_qs(parsed_url.query)
    seg_seed_id = "%s.%s.%s.%s" % \
                  (qdic['net'][0],
                   qdic['sta'][0],
                   qdic.get('loc', [''])[0],
                   qdic['cha'][0])

    # build name from event (dirty way):
    fname = "mag=%s;magt=%s;lat=%s;lon=%s;depth=%s;time=%s;id=%s" % \
            (row['evt_mag'], row['evt_magtype'], row['evt_lat'], row['evt_lon'],
             row['evt_depth_km'], row['evt_time'], row['Segment.db.id'])
    # build destpath:
    class_label = row['class']
    destfilepath = os.path.join(rootdir, class_label, seg_seed_id,
                                fname + '.mseed')

    # make dir if it doesn't exist:
    if not isdir(dirname(destfilepath)):
        os.makedirs(dirname(destfilepath))

    # save trace (no overwrite):
    if not isfile(destfilepath):
        stream = read_mseed(url)
        if len(stream) != 1:
            raise ValueError("%d traces in stream, maybe due to gaps/overlaps"
                             % len(stream))
        trace = stream[0].detrend(type='linear')
        # let's be paranoid:
        if trace.get_id() != seg_seed_id:
            raise ValueError('Requested miniSEED ID differs from actual one')
        trace.write(destfilepath, format="MSEED")


def read_mseed(url):
    """Reads URL and returns the miniSEED"""
    return read(url, format="MSEED")


if __name__ == '__main__':
    # invoke this script as python download.py. Parse arguments and download:
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
    print('(Please wait, several minutes might be needed)')
    print()

    with open(join(dirname(__file__), 'urls.csv'), newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        saved = 0
        for row in reader:
            try:
                download_waveform(destdir, row)
                saved += 1
            except Exception as exc:
                print('WARNING: Segment not saved (%s). URL: %s' % (str(exc), row['url']))
        print()
        print('Done, %d segment saved under %s' % (saved, ))



