import logging

from slugify import slugify
from sqlalchemy import create_engine

from bonobo_sqlalchemy.write import DatabaseLoad

logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

engine = create_engine('sqlite:///basics.sqlite')

import json

from blessings import Terminal

from bonobo import console_run, JsonWriter
from bonobo.basics import Tee
from bonobo.ext.opendatasoft import from_opendatasoft_api

try:
    import pycountry
except ImportError as exc:
    raise ImportError('You must install package "pycountry" to run this example.') from exc

API_DATASET = 'fablabs-in-the-world'
API_NETLOC = 'datanova.laposte.fr'
ROWS = 100

t = Terminal()


def _getlink(x):
    return x.get('url', None)


def normalize(row):
    result = {
        **
        row,
        'links': list(filter(None, map(_getlink, json.loads(row.get('links'))))),
        'country': pycountry.countries.get(alpha_2=row.get('country_code', '').upper()).name,
    }
    return result


def filter_france(row):
    if row.get('country') == 'France':
        yield row


def prepare_for_database(row):
    address = list(
        filter(
            None, (
                ' '.join(filter(None, (row.get('postal_code', None), row.get('city', None)))),
                row.get('county', None),
                row.get('country'),
            )
        )
    )
    yield {
        'slug': slugify(
            row.get('name'), only_ascii=True
        ),
        'name': row.get('name'),
        'address': address,
        'geometry': row.get('geometry'),
    }


if __name__ == '__main__':
    console_run(
        from_opendatasoft_api(
            API_DATASET, netloc=API_NETLOC, timezone='Europe/Paris'
        ),
        normalize,
        filter_france,
        Tee(prepare_for_database),
        DatabaseWriter('fablabs.json'),
        output=True,
    )

#load = DatabaseLoad(engine=engine, table_name='coffeeshops', )
