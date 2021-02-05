# double_event_dataset

## Required Python packages:
```
numpy>=1.19.5
obspy>=1.2.2
pandas>=1.2.1
```
<!-- internal usage:
python createurls.py
-->

## Usage

Tha data size (skipping the generation of all artificial double events)
is in the order of Gigabytes. Therefore, data must be downloaded and
processed inplace. This program stores the URL of labelled waveforms and let
the user manipulate them.

Here a snippet of the functions to use in your code
(your Python project, IPython in the terminal, Notebook and so on)

```python
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
    # This is your channel_data object, a collection
    # of recorded Waveforms from given channel, and
    # annotated with a class label.

    # Few preliminary information:
    # ============================

    # 1. Get the channel_data id is the channel identifier in the usual form
    # "<netowrk_code>.<station_code>.<location_code>.<channel_code>":
    channel_id = channel_data.id  # e.g. 3A.MZ01..EHE
    
    # 2. Get the total number of wavefomrs (ObsPy Traces objects) in the given channel:
    ntraces = channel_data.numtraces

    # 3. Access of all channel waveforms (ObsPy Traces). For instance, you might
    # want to apply a preprocess such e.g. a detrend (necessary below for the creation 
    # of artificial double event by merging two waveform events later).
    for trace in channel_data.traces:
        trace.detrend(type='linear')
    # WARNING: Accessing `channel-data.traces` from now on will return the traces
    # detrend(ed)!! As many ObsPy methods, `detrend` above permanently modifies 
    # the Trace data, use with care! (if you do not want to do this,
    # you can preprocess a copy of each Trace, see below):
    
    # 3. Access and inspect all traces metadata (pandas Series, or dict-like object):
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

        # Example:
        event_time = metadata['time']  # event time, Timestamp (datetime) object
        event_time = metadata.time  # same as above
        event_mag = metadata.mag
        event_lat, event_lon = metadata['lat'], metadata['lon']
        dist_deg = metadata.dist_deg  # event to station distance (in degrees)
        event_depth = metadata['depth_km']
    
    # You can also get each trace coupled with its metadata in loops, e.g.:
    # for trace, metadata in zip(channel_data.traces, channel_data.metadata):
    #   ... execute your code here ...

    # Multi event creation:
    # =====================

    # Multi event creation can be easily performed with a shortcut method
    # that yields all traces pairs (2-combinations of traces) from the same channel:
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
        # take NUM_MULTIEVENT random point indices from max_trace:
        pts = np.random.choice(len(maxtrace), NUM_MULTIEVENT, p=None)
        # p above is a probability distribution with length=len(max_t).
        # None means: use linear distribution. Change as you like 
        # (see numpy doc in case)
        
        # create multi event traces:
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
            new_metadata.npts = new_data_len 
            # now create the Trace:
            new_trace = Trace(new_data, header=new_metadata)
            # append Trace:
            multievent_traces.append(new_trace)
            # Now it's up to the user: you can continue the processing
            # and save the result and/or save the newly created Trace
            # now. A good unique file name could be created by merging
            # the two source segment ids. These ids refer to the source
            # database and serve the purpose of providing unique identifiers 
            # for all waveforms:
            filename = "%d-%d.mseed" % (metadata1._segment_db_id,
                                        metadata2._segment_db_id)
            # and then save it wherever you want ...
```