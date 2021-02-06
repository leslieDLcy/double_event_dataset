"""
Utilities for creating double events
"""
from datetime import datetime
from itertools import combinations
from os import listdir
from os.path import join, isdir, isfile, basename, dirname
from os import makedirs

import pandas as pd
from obspy import read


_dataframe = None  # singleton for the input dataframe


URLS_PATH = join(dirname(dirname(__file__)), 'urls.csv')


def get_input_dataframe():
    """Return the input dataframe where all URLs of labelled segments are
    stored. The dataframe has the following columns:

    - _segment_db_id: int. Segment database id (for debugging purposes only,
      useful only to get back to the original database segment, or to provide
      a waveform unique id)
    - url: str. The base URL of the data center.
    - classlabel: str. the annotated class label. Can be 'urb_multi', 'urb_nc',
      'urb_single'. See also :func:`class_labels()`
    - mag: float. Magnitude
    - magtype: str. Magnitude type
    - lat: float Event latitude
    - lon: float. Event longitude
    - depth_km: float. Event depth (in Km)
    - time: datetime (pandas Timestamp). Event time
    - dist_deg: float. Distance event/ recording station, in degrees
    - net: str. Station network code
    - sta: str. Station station code
    - loc: str. Station location code
    - cha: str. Station channel code
    - starttime: datetime (pandas Timestamp). Waveform segment requested
      (theoretical) start time
    - endtime: datetime (pandas Timestamp). waveform segment requested
      (theoretical) end time
    """
    global _dataframe
    if _dataframe is None:
        _dataframe = _load_input_dataframe()
    return _dataframe


def _load_input_dataframe():
    file = URLS_PATH
    dfr = pd.read_csv(file)
    # convert NaNs in loc to empty string:
    dfr.loc[pd.isna(dfr['loc']), 'loc'] = ''
    # We could have preserved empty string sin 'loc' column by passingw
    # keep_default_na=False in read_csv, but this affects also numeric columns,
    # where we do want empty cells to be parsed as NaN

    for col in ('starttime', 'endtime', 'time'):
        assert not pd.isna(dfr[col]).any()  # just a check
        dfr[col] = pd.to_datetime(dfr[col])
    for col in ['net', 'sta', 'loc', 'cha', 'magtype', 'classlabel', 'url']:
        assert not pd.isna(dfr[col]).any()  # just a check
        dfr[col] = dfr[col].astype('category')
    # to be sure, use a sequential integer index:
    dfr.reset_index(drop=True, inplace=True)
    return dfr


def class_labels():
    """Return the class labels in the input dataset (list of strings)"""
    return get_input_dataframe().classlabel.cat.categories.to_list()


def get_events(root_dir=None, classlabel=None, verbose=False):
    """Yield data objects of this dataset, grouped by channels.
    Each yielded data object is a collection of all waveforms recorded at a
    specific seismic channel.

    :param root_dir: the root directory where to store/cache each downloaded
        miniSEED (in a nested directory structure), so that further call of
        this function (with the same same `root_dir` argument) will be faster,
        because the miniSEED will be loaded from disk when found, instead that
        from URLs
    :param classlabel: a string denoting a classlabel to match (e.g. 'urb_multi',
        or 'urb_single') or a list of classlabel strings (=match any of the
        given labels). None (the default) means: return all events
    :param verbose: if additional output has to be printed to the standard
        output
    """
    dfr = get_input_dataframe()
    if isinstance(classlabel, str):
        dfr = dfr[dfr.classlabel == classlabel].copy()
    elif isinstance(classlabel, (list, tuple)):
        dfr = dfr[dfr.classlabel.isin(classlabel)].copy()

    for [net, sta, loc, cha], _df in dfr.groupby(['net','sta', 'loc', 'cha']):
        channel_id_str = '%s.%s.%s.%s' % (net, sta, loc, cha)
        data = []
        for index_, series in _df.iterrows():
            file_url = series['url'] + "?net=" + net + "&sta=" + sta + \
                        "&loc=" + loc + "&cha=" + cha + \
                        "&starttime=" + series['starttime'].isoformat('T') + \
                        "&endtime=" + series['endtime'].isoformat('T')
            file_path = None
            if root_dir:
                filename = "mag=%s_lat=%s_lon=%s_depth=%s_time=%s" % \
                           (str(series['mag']), str(series['lat']),
                            str(series['lon']), str(series['depth_km']),
                            series['time'].isoformat('T'))
                file_path = join(root_dir, channel_id_str, filename + '.mseed')

            stream = read_stream(file_path)
            if stream is None:
                stream = read_stream(file_url)
                if stream is not None and file_path:
                    if not isdir(dirname(file_path)):
                        makedirs(dirname(file_path))
                    try:
                        stream.write(file_path, format='MSEED')
                    except:
                        if verbose:
                            print('Could not write to file: %s' % file_path)
            if stream is None:
                if verbose:
                    print('Channel %s: discarding waveform (could not read as '
                          'miniSEED). URL: %s' %
                          (channel_id_str, file_url))
                continue
            if len(stream) != 1:
                if verbose:
                    print('Channel %s: discarding waveform (several traces '
                          'found, maybe gaps/overlaps?). URL: %s' %
                          (channel_id_str, file_url))
                continue
            trace = stream[0]
            data.append((trace, series))
        yield ChannelTracesCollection(net, sta, loc, cha, data)


def read_stream(file_or_url, raise_on_err=False):
    """Wrapper around ObsPy.read, does not raise and returns None in case of
    failure if `raise_on_err=False` (the default)
    """
    if not file_or_url:
        if raise_on_err:
            raise ValueError('Can not read Stream: %s invalid path' %
                             file_or_url)
        return None
    try:
        return read(file_or_url, format='MSEED')
    except:  # noqa
        if raise_on_err:
            raise
        return None


class ChannelTracesCollection:
    """Container for a channel and its recorded waveforms"""
    def __init__(self, net, sta, loc, cha, data):
        self.data = data
        self.cha = cha
        self.net = net
        self.sta = sta
        self.loc = loc

    @property
    def numtraces(self):
        return len(self.data)

    @property
    def id(self):
        return self.net + "." + self.sta + "." + self.loc + "." + self.cha

    @property
    def traces(self):
        return (_[0] for _ in self.data)

    @property
    def metadata(self):
        return (_[1] for _ in self.data)

    @property
    def pairs(self):
        return combinations(self.data, 2)


if __name__ == "__main__":

    cache_dir = join(dirname(dirname(__file__)), 'tmp', 'downloaded')

    import numpy as np
    from obspy import Trace
    from double_event_dataset.utils import get_events

    # Setup a cache directory. Use this if you want all waveforms to be
    # saved also on your computer and/or make the `get_events` method
    # below run faster next time (providing the same cache dir will load from
    # disk instead than from the web)
    cache_dir = None  # None: no cache (always download waveforms)
    # Provide a class label to work only on specific waveforms.
    # None means: all labels
    classlabel = 'urb_single'

    for channel_data in get_events(cache_dir, classlabel=classlabel):
        # this is your channel_data object, a collection
        # of labelled ObsPy traces of a given channel.

        # Few preliminary information:

        # 1. the channel_data id is the channel identifier
        # in the usual form
        # "<netowrk_code>.<station_code>.<location_code>.<channel_code>":
        channel_data.id  # e.g. 3A.MZ01..EHE

        # 2. You can access all channel traces (ObsPy trace object representing a
        # waveform) and their metadata (pandas Series, dict-like objects) which includes
        # earthquake information.

        # 2a. E.g., Let's access all traces to apply now a preprocess, such as a
        # detrend (necessary below for the creation of artifical double event by merging
        # two waveform events later).
        for trace in channel_data.traces:
            trace.detrend(type='linear')
        # WARNING: Accessing `channel-data.traces` from now on will returns the traces
        # detrend(ed)!! As many ObsPy methods, `detrend` above permanently modifies
        # the Trace data, use with care! (if you do not want to do this,
        # you can preprocess a copy of each Trace, see below):

        # 2b. Let's inspect the trace metadata, which has also some information
        # about the earthquake:
        for metadata in channel_data.metadata:
            # Relevant keys/attributes are:
            # Attribute                                                  Value
            # _segment_db_id                                           2618452
            # url            http://webservices.ingv.it/fdsnws/dataselect/1...
            # classlabel                                             urb_multi
            # mag                                                          3.0
            # magtype                                                       ML
            # lat                                                      42.8562
            # lon                                                      13.2533
            # depth_km                                                     8.5
            # time                                         2016-10-11 07:32:50
            # dist_deg                                                0.185072
            # starttime                                    2016-10-11 07:31:55
            # endtime                                      2016-10-11 07:35:55
            #

            # Example:
            event_time = metadata['time']  # Timestamp (datetime) object
            event_mag = metadata['mag']
            event_lat, event_lon = metadata['lat'], metadata['lon']

        # Multi event creation:

        # Multi event creation is up to the user but we created
        # a shortcut method that yields traces pairs with
        # their metadata. So:
        for (trace1, metadata1), (trace2, metadata2) in channel_data.pairs:

            # If you did not pass 'urb_single' to the `classlabel` argument
            # above, you might want to create multi events only if the two
            # traces are labelled as single event, e.g.:
            if (metadata1.classlabel, metadata2.classlabel) != \
                    ('urb_single', 'urb_single'):
                continue

            # traces from the same channel should have the same sampling
            # rate. However, let's check, if they have different sampling
            # rates, either interpolate, or let's keep it simple, skip:
            if trace1.stats.delta != trace2.stats.delta:
                continue

            # If the traces are still "raw" (e.g., you did not perform
            # any detrend on all traces, see above), you might want
            # to apply some processing here, but do it on a copy so that
            # you do not permanently modify the traces. E.g.:
            trace1 = trace1.copy().detrend(type='linear')
            trace2 = trace2.copy().detrend(type='linear')

            # Now you can start compose them
            # define how many artificial multi event traces you want:
            NUM_MULTIEVENT = 3
            # take the trace with more points:
            maxtrace, mintrace = (trace1, trace2) if len(trace1) >= len(trace2) \
                else (trace2, trace1)
            # take NUM_MULTIEVENT random points from max_trace:
            pts = np.random.choice(len(maxtrace), NUM_MULTIEVENT, p=None)
            # p above is a probability distribution with length=len(max_t).
            # None means: use linear distribution.
            # Change as you like (see numpy doc in case)
            multievent_traces = []
            for pt_ in pts:
                # now overlap mintrace over maxtrace, starting at index `pt_`
                # (maxtrace[pt_:])
                new_data_len = max([len(maxtrace), pt_ + len(mintrace)])
                new_data = np.zeros(new_data_len, dtype=float)
                new_data[:len(maxtrace)] = maxtrace.data
                new_data[pt_: pt_ + len(mintrace)] = mintrace.data
                # create new ObsPy Trace
                # first update the stats (Trace metadata)
                new_metadata = maxtrace.stats.copy()
                new_metadata.npts = new_data_len  # update stats and pass them:
                new_trace = Trace(new_data, header=new_metadata)
                # append Trace:
                multievent_traces.append(new_trace)
                # or create your data (e.g. spectrogram),
                # or gain, save to file, maybe using event information from
                # 'metadata'. E.g., a quik unique value might be using the
                # _segment_id (database ID):
                filename = "%d-%d.mseed" % (metadata1._segment_db_id,
                                            metadata2._segment_db_id)
                # and then save it wherever you want ...
        break