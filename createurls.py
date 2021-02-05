from os.path import join, dirname, basename, abspath, isdir, realpath
from os import listdir

from stream2segment.main import process
from stream2segment.utils import inputargs
# from double_event_dataset.utils import COLUMNS


if __name__ == "__main__":
    root = dirname(realpath(__file__))

    db_url = open(join(root, 's2s', 'dburl.txt')).read().strip()
    files_basename = join(root, 's2s', 'double_event_dataset')
    logfile = join(root, 's2s', basename(__file__) + ".log")

    # create root (and later append destdirname):
    # root = join(root, 'data')

    # check (skip, now overwrite only missing file(s)):
    # if isdir(root):
    #     if len(listdir(root)):
    #         raise SystemError("%s already exist but not empty" % root)
    # else:
    #     raise SystemError("%s does not exist" % root)

    outfile = join(root, 'urls.csv')

    process(db_url, pyfile=files_basename + '.py',
            config=files_basename + '.yaml',
            log2file=logfile, verbose=True, outfile=outfile,
            # yaml parameter to overwrite:
            destdir=root,
            #, segment_select={'id': '<4000000'}
            )
