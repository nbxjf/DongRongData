import logging.config

from common import proxy
from persist import DbHelper
from persist import EsHelper
import os


def initialize(terms=()):
    term_logger, term_database, term_es, term_proxy = 'logger', 'database', 'es', 'proxy'

    prefix = os.path.abspath(os.path.dirname(__file__))

    paths = {
        term_logger: prefix + '/config/config-logger.ini',
        term_database: prefix + '/config/config-database.ini',
        term_es: prefix + '/config/config-es.ini',
        term_proxy: prefix + '/config/config-proxy.ini',
    }

    for term in terms:
        if term == term_logger:
            logging.config.fileConfig(paths[term])
        elif term == term_database:
            DbHelper.init_pool(paths[term])
        elif term == term_es:
            EsHelper.init(paths[term])
        elif term == term_proxy:
            proxy.init(paths[term])
        else:
            raise Exception('term error')
    logging.info('initial procedure ends')


if __name__ == '__main__':
    initialize(('es', 'database'))
