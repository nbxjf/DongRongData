# coding=utf-8
import sys
import os


def resolve():
    if os.path.basename(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))) == 'IntegratedJob':
        sys.path.append('../../')
    elif os.path.basename(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir))) == 'IntegratedJob':
        sys.path.append('../../../')
    elif os.path.basename(os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir, os.pardir))) == 'IntegratedJob':
        sys.path.append('../../../../')
    else:
        raise Exception('system path not unresolved')

    reload(sys)
    sys.setdefaultencoding('utf-8')
