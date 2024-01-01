import sys, os


def file_root():
    path_ = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, path_)
    return path_