from os.path import join, dirname, basename, abspath, isdir
from os import listdir

from stream2segment.main import process

if __name__ == "__main__":
    root = dirname(__file__)
    db_url = open(join(root, 'dburl.txt')).read().strip()
    file = join(root, 'double_event_dataset')
    logfile = join(root, basename(__file__) + ".log")

    # create root (and later append destdirname):
    root = join(abspath(root, 'data'))
    if isdir(root):
        if len(listdir(root)):
            raise SystemError("%s already exist but not empty" % root)
    else:
        raise SystemError("%s does not exist" % root)

    process(db_url, pyfile=file + '.py', config=file + '.yaml',
            log2file=logfile, verbose=True, destdir=root)
