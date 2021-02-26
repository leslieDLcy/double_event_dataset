"""
Utilities for creating double events
"""
from datetime import datetime
from itertools import combinations
from os import listdir
from os.path import join, isdir, isfile, basename, dirname
from os import makedirs

import pandas as pd
from obspy import read, read_inventory as _read_inventory


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
    
    # Problem: the "loc" column is interpreted as float.
    # which causes the following outcomes:
    # 1: '01' is converted to 1.0 (same problem for '00', '02' and so on)
    # 2: empty strings converted to nan
    # To solve 1., we can pass dtype=str to read_csv,
    # To solve 2., we can pass keep_default_na=False to read_csv
    # However, both solutions affect all columns, so
    # we need to reload the "loc" column only from the csv:
    loc_col = pd.read_csv(file, usecols=['loc'], keep_default_na=False, dtype=str)['loc']
    # and set it to the dataframe
    dfr['loc'] = loc_col
    # for safety:
    dfr.loc[pd.isna(dfr['loc']), 'loc'] = ''

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


def get_channel_data(root_dir=None, classlabel=None, verbose=False):
    """Yield data objects of this dataset, grouped by channels.
    Each yielded data object is a collection of all waveforms recorded at a
    specific seismic channel.

    :param root_dir: the root directory where to store/cache each downloaded
        miniSEED (in a nested directory structure), so that further call of
        this function (with the same same `root_dir` argument) will be faster,
        because the miniSEED will be loaded from disk when found, instead that
        from URLs
    :param classlabel: a string denoting a class label to match (e.g.
        'urb_multi', or 'urb_single', see :func:`class_labels`) or a list of
        class label strings (=match any of the given labels).
        None (the default) means: return all events
    :param verbose: if additional output has to be printed to the standard
        output
    """
    dfr = get_input_dataframe()
    if isinstance(classlabel, str):
        dfr = dfr[dfr.classlabel == classlabel].copy()
    elif isinstance(classlabel, (list, tuple)):
        dfr = dfr[dfr.classlabel.isin(classlabel)].copy()

    for [net, sta], sta_df in dfr.groupby(['net', 'sta']):
        inv = None
        inv_path = None
        if root_dir:
            inv_path = join(root_dir, 'station_inventories',
                            '%s.%s.xml' % (net, sta))

            if isfile(inv_path):
                inv = read_inventory(inv_path)

        if inv is None:  # try from URL
            sta_url = sta_df.at[sta_df.index[0], 'url']
            sta_url = sta_url.replace('dataselect', 'station')
            sta_url += "?net=" + net + "&sta=" + sta + "&level=response"
            inv = read_inventory(sta_url)
            if inv is None:
                print('Could not read inventory, discarding all related '
                      'waveforms. URL: %s' % sta_url)

        if inv is None:
            continue

        if inv_path and not isfile(inv_path):
            if not isdir(dirname(inv_path)):
                makedirs(dirname(inv_path))
            inv.write(inv_path, format='STATIONXML')

        for [loc, cha], cha_df in sta_df.groupby(['loc', 'cha']):
            channel_id_str = '%s.%s.%s.%s' % (net, sta, loc, cha)
            data = []
            for index_, series in cha_df.iterrows():
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
            yield ChannelTracesCollection(net, sta, loc, cha, data, inv)


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


def read_inventory(file_or_url, raise_on_err=False):
    if not file_or_url:
        if raise_on_err:
            raise ValueError('Can not read Inventory: %s invalid path' %
                             file_or_url)
        return None
    try:
        return _read_inventory(file_or_url, format='STATIONXML')
    except:  # noqa
        if raise_on_err:
            raise
        return None


class ChannelTracesCollection:
    """Container for a channel and its recorded waveforms"""
    def __init__(self, net, sta, loc, cha, data, inventory):
        self.data = data
        self.cha = cha
        self.net = net
        self.sta = sta
        self.loc = loc
        self.inventory = inventory

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
