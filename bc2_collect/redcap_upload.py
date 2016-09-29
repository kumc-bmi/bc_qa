'''redcap_upload -- chunked redcap uploader using API credentials

'''

from collections import namedtuple
from datetime import date, datetime
from itertools import groupby
from pprint import pformat
from urllib import urlencode
from urllib2 import HTTPError
import ConfigParser
import StringIO
import csv
import json
import logging
import re

log = logging.getLogger(__name__)


def _integration_test(argv, open_arg, arg_engine, ua):
    config_fn, db_fn = argv[1:3]

    chunk_size, project = Project._from_config(
        config_fn, open_arg(config_fn), ua)

    db = arg_engine(db_fn)

    # Only import data_chunks here
    # avoid circular import in normal usage.
    from edc_summary import data_chunks
    REDCapData.upload(project, data_chunks(chunk_size, db))


class Project(object):
    '''REDCap API proxy to handle import_records()

    Parameter names are taken from **Import Records**
    in ''REDCap API Documentation''

    /redcap/api/help/?content=imp_records

    '''
    def __init__(self, ua, url, api_token):
        def import_records(data, format='csv'):
            if format != 'csv':
                raise NotImplementedError(format)
            form = [('token', api_token),
                    ('content', 'record'),
                    ('format', format),
                    ('type', 'eav'),
                    ('data', data),
                    ('returnFormat', 'json')]
            log.info('sending: %s', [(k, v[:15]) for (k, v) in form])
            log.debug('sending: %s', data)
            try:
                reply = ua.open(url, urlencode(form))
            except HTTPError as err:
                body = err.read()
                try:
                    body = json.loads(body)
                except:
                    pass
                log.error('code: %d\n%s', err.code, pformat(body))
                raise
            if reply.getcode() == 200:
                result = json.load(reply)
                log.debug('result: %s', result)
                if 'count' in result:
                    return result
                raise IOError(result)

            raise IOError(reply.getcode())
        self.import_records = import_records

    @classmethod
    def _from_config(cls, config_fn, config_fp, ua,
                    section='redcap', verify_ssl=False):
        # undocumented; only used in integration testing.
        conf = ConfigParser.SafeConfigParser()
        conf.readfp(config_fp, config_fn)

        [url, api_key] = [conf.get(section, k)
                          for k in ['url', 'api_key']]

        if not api_key:
            raise IOError('empty api_key')

        project = cls(ua, url, api_key)

        chunk_size = conf.getint(section, 'chunk_size')
        return chunk_size, project


# Map PyCap names to REDCap data dictionary column names
DICT_COLS = [
    ('field_name', 'Variable / Field Name'),
    ('form_name', 'Form Name'),
    ('section_header', 'Section Header'),
    ('field_type', 'Field Type'),
    ('field_label', 'Field Label'),
    ('select_choices_or_calculations',
     'Choices, Calculations, OR Slider Labels'),
    ('field_note', 'Field Note'),
    ('text_validation_type_or_show_slider_number',
     'Text Validation Type OR Show Slider Number'),
    ('text_validation_min', 'Text Validation Min'),
    ('text_validation_max', 'Text Validation Max'),
    ('identifier', 'Identifier?'),
    ('branching_logic', 'Branching Logic (Show field only if...)'),
    ('required_field', 'Required Field?'),
    ('custom_alignment', 'Custom Alignment'),
    ('question_number', 'Question Number (surveys only)'),
    ('matrix_group_name', 'Matrix Group Name'),
]


class FieldDef(
        namedtuple('FieldDef',
                   [n for (n, _) in DICT_COLS])):
    # TODO: test for constraint "Form names must be sequential and
    # cannot repeat again after being used for another form. "
    full_column_names = [lbl for (_, lbl) in DICT_COLS]

    @classmethod
    def _default(cls):
        return cls(*[None] * len(cls._fields))

    @classmethod
    def mkv(cls, name, x='', xty=None,
            section=None, ty='text', validation='', label=None):
        '''
        >>> mkv = FieldDef.mkv
        >>> v = mkv('x', x=1)
        >>> [col for col in v if col]
        ['x', 'text', 'x', 'integer']

        >>> v = mkv('ht', label="Height", x=1.1)
        >>> [col for col in v if col]
        ['ht', 'text', 'Height', 'number']

        >>> v = mkv('abc', label='def')
        >>> [col for col in v if col]
        ['abc', 'text', 'def']

        >>> v = mkv('color', x=[('r', 'red'), ('g', 'green'), ('b', 'blue')])
        >>> print v.select_choices_or_calculations
        r, red
        g, green
        b, blue

        >>> v = mkv('nochoice', x=[])
        >>> [col for col in v if col]
        ['nochoice', 'dropdown', 'nochoice', '1, N/A']

        '''
        xty = type(x) if xty is None else xty

        ty, validation = (
            ('dropdown', '') if xty is list
            else
            (ty, 'date_ymd') if xty is date
            else
            (ty, 'datetime_seconds_ymd') if xty is datetime
            else
            (ty, 'integer') if xty is int
            else
            (ty, 'number') if xty is float
            else (ty, validation))
        choices = (cls.encode_choices(pairs=x)
                   if isinstance(x, type([])) else None)

        return cls._default()._replace(
            field_name=name,
            section_header=section,
            field_type=ty,
            text_validation_type_or_show_slider_number=validation,
            field_label=label or name,
            select_choices_or_calculations=choices)

    @classmethod
    def mkdict(cls, form, entries):
        '''Collect entries on a form.

        :param form: name of REDCap CRF "page" where these entries go
        :param entries: list FieldDef's
        '''
        return [entry._replace(form_name=form)
                for entry in entries]

    def choices(self):
        items = self.select_choices_or_calculations.split('\\n')
        return [item.strip().split(', ', 1) for item in items if item]

    @classmethod
    def encode_choices(cls, labels=None, pairs=None,
                       default=['N/A']):
        '''
        >>> choices = [(1, u'Colon'),
        ...            (2, u'Spleen')]

        >>> print FieldDef.encode_choices(pairs=choices)
        1, Colon
        2, Spleen

        >>> choices = ['a', 'b', 'c']
        >>> print FieldDef.encode_choices(labels=choices)
        1, a
        2, b
        3, c

        .. note:: "Multiple choice fields can only have coded values
                  (in the choices column F) that are numeric or
                  alpha-numeric (lower case or upper case, with or
                  without underscores), thus they cannot have codings
                  that contain spaces or other characters."

        "Each multiple choice field (radio, drop-down, checkbox, etc.)
        must have choices listed in column F"

        >>> FieldDef.encode_choices(pairs=[])
        '1, N/A'

        '''
        pairs = pairs or [(str(ix + 1), l)
                          for (ix, l) in
                          enumerate(labels or default)]
        return (
            '\n'.join(
                ['%s, %s' % (code,
                             label.replace(',', '*').replace('|', '*'))
                 for (code, label) in pairs]))

    @classmethod
    def name_from_hint(cls, n, suffix_length=0,
                       max_length=26):
        '''Make a valid RedCap field name.

        Only lower case chars, numbers, and underscores are allowed.
        Also, field names can't start with a number nor underscore
        character.

        Max length is 26

        >>> len(FieldDef.name_from_hint(
        ...     'x1234567890123456789012345678901234567890'))
        26

        >>> FieldDef.name_from_hint('_stuff___stuff_')
        'stuff_stuff'

        '''
        n = str(re.sub("[^0-9a-z_]+", "_", n.lower()))
        n = re.sub("^[^a-z]+", "", n)  # get rid of leading non-letters
        n = n[:max_length - suffix_length]
        n = re.sub("_{2,}", "_", n)  # condense multiple consecutive '_'
        n = re.sub("_+$", "", n)  # get rid of trailing '_'

        return n if n > '' else 'x'

    @classmethod
    def save_metadata(cls, wr, metadata):
        with wr.outChannel() as outf:
            sink = csv.writer(outf)
            sink.writerow(cls.full_column_names)
            sink.writerows(metadata)

    @classmethod
    def load_metadata(cls, rd):
        with rd.inChannel() as inf:
            source = csv.reader(inf)
            source.next()  # skip header
            return [cls(*row) for row in source]


class REDCapData(namedtuple('redcap_data',
                            ['record',
                             'field_name',
                             'value',
                             'redcap_event_name'])):
    @classmethod
    def form_complete(cls, record_id, form, event):
        return REDCapData(record_id,
                          field_name='%s_complete' % form,
                          value='1',
                          redcap_event_name=event)

    @classmethod
    def save(cls, wr, data):
        with wr.outChannel() as otf:
            cls.write(otf, data)

    @classmethod
    def write(cls, out, data,
              longitudinal=True):
        sink = csv.writer(out)
        schema = cls._fields if longitudinal else cls._fields[:-1]
        sink.writerow(schema)
        sink.writerows(data)

    @classmethod
    def load(cls, rd):
        with rd.inChannel() as inf:
            source = csv.reader(inf)
            source.next()  # skip header
            return [cls._make(row) for row in source]

    @classmethod
    def data_chunks(cls, data, chunk_size):
        offset = 0
        while offset < len(data):
            yield data[offset:offset + chunk_size]
            offset += chunk_size

    @classmethod
    def upload(cls, project, chunks,
               longitudinal=False):
        qty = 0
        for chunk in chunks:
            qty += len(chunk)
            log.info('importing %d values (total: %s)', len(chunk), qty)

            chunk_sink = StringIO.StringIO()
            cls.write(chunk_sink, chunk, longitudinal)
            result = project.import_records(chunk_sink.getvalue(),
                                            format='csv')

            if 'error' in result or 'count' not in result:
                raise IOError(result)
            log.info('imported %s records', result['count'])
        return qty

    @classmethod
    def upload_check(cls, data, key_field):
        records = [(record_id, dict([(d.field_name, d.value)
                                     for d in rec_data]))
                   for (record_id, rec_data) in
                   groupby(sorted(data, key=lambda d: d.record),
                           key=lambda d: d.record)]

        by_mrn = [(mrn, list(pat_records))
                  for (mrn, pat_records) in
                  groupby(sorted(records, key=lambda r: r[1].get(key_field)),
                          key=lambda r: r[1].get(key_field))]
        bad_mrns = [(mrn, ds)
                    for (mrn, ds) in by_mrn if mrn and len(ds) > 1]
        if bad_mrns:
            log.error('bad MRNs: %s', pformat(bad_mrns))


if __name__ == '__main__':
    def _script(level=logging.INFO):
        from sys import argv
        from urllib2 import build_opener
        from sqlalchemy import create_engine

        def open_arg(path):
            logging.basicConfig(level=level)
            if path not in argv:
                raise IOError('not arg: %s' % path)
            return open(path)

        _integration_test(
            argv=argv[:],
            open_arg=open_arg,
            arg_engine=lambda fn: create_engine('sqlite:///' + fn),
            ua=build_opener())
    _script()
