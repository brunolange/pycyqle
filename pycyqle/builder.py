"""
"""

from exos import ueach

__author__ = "Bruno Lange"
__license__ = "MIT"
__version__ = "0.0.1"
__maintainer__ = "Bruno Lange"
__email__ = "blangeram@gmail.com"
__status__ = "Development"

def param_build(cls, **kwargs):
    return dict_build(cls, kwargs)


def dict_build(cls, dic):
    obj = cls()
    ueach(lambda k, v: getattr(obj, k)(v), dic.items())
    return obj
