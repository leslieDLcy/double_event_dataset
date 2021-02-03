from os.path import join, dirname, basename, abspath, isdir
from os import listdir

from stream2segment.main import process

from utils import COLUMNS

if __name__ == "__main__":
    root = dirname(__file__)

    db_url = open(join(root, 's2s', 'dburl.txt')).read().strip()
    file = join(root, 's2s', 'double_event_dataset')
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
    process(db_url, pyfile=file + '.py', config=file + '.yaml',
            log2file=logfile, verbose=True, outfile=outfile,
            # yaml parameter to overwrite:
            destdir=root, columns=COLUMNS
            #, segment_select={'id': '<4000000'}
            )
