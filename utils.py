"""
Utilities for creating double events
"""
from datetime import datetime
from os import listdir
from os.path import join, isdir, isfile, basename, dirname

from obspy import read

single = 'urb_single'

# any new column: add a name: type
# Add the parsing here
# add the serialization in double_event_dataset
COLUMNS = {
    'lat': float,
    'lon': float,
    'depth_km': float,
    'mag': float,
    'magtype': str,
    'time': datetime   # lambda x: datetime.fromisoformat(x),
}


def get_events(rootdir):
    """Yield data objects found in in `rootdir`, grouped by channels.
    Each yielded data object represents a channel and is a tuple of the form:
    (net, sta, loc, cha, events)
    where net, sta, loc, cha are strings denoting the network code, station code
    location, channel code of the given channel, and events is a list of dicts
    representing the recorded events at the given channel. Each dict has the
    following attributes:
    """
    scan_dir = join(rootdir, single)  # scan single events only
    for channel_dirname in listdir(scan_dir):
        channel_dir = join(scan_dir, channel_dirname)
        if isdir(channel_dir):
            events = []
            net, sta, loc, cha = basename(channel_dir).split('.')
            for fle in listdir(channel_dir):
                stream = read(join(channel_dir, fle), format="MSEED")
                if len(stream) != 1:
                    continue
                ret = {'trace': stream[0]}
                for chunk in fle.split(';'):
                    attname, attval = chunk.split('=')
                    if attname == 'evt_time':
                        attval = datetime.fromisoformat(attval)
                    elif attname in ('evt_lat', 'evt_lon', 'evt_depth_km',
                                     'mag'):
                        attval = float(attval)
                    ret[attname] = attval
                events.append(ret)
            yield net, sta, loc, cha, events


get_events.__doc__ += "\n" + "\n".join("%s: %s" % (k, str(v))
                                       for k, v in COLUMNS.items())


if __name__ == "__main__":
    print(get_events.__doc__)
    for events in get_events(join(dirname(__file__), 'tmp')):
        break