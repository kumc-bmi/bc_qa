'''i2b2_pset_build -- build patient set from study ids

Usage:
  i2b2_pset_build [options] crosswalk <consented> <survey_order> <crosswalk>
  i2b2_pset_build [options] patient-map <cohort_mrns> <patient_nums>
  i2b2_pset_build [options] add-result <patients> <dummy_query>

Options:
  crosswalk             map study_id to current patient_num via MRN
  <consented>           CSV file with study_id, order_id columns
  <survey_order>        CSV file with order_id, mrn columns
  <crosswalk>           CSV file to save study_id,patient_num,date_shift
  patient-map           map mrns to patient_num
  <cohort_mrns>         CSV file with mrn column
  <patient_nums>        CSV file to save mapped patient_num results
  --id-schema=SCHEMA    schema for identified patient_mapping table
                        [default: NIGHTHERONDATA]
  --mrn-source CODE     i2b2 patient_ide_source used for MRNs
                        in the patient_mapping table
                        [default: SMS@kumed.com]
  --id-key NAME         name of environment variable to find
                        SQLAlchemy DB URL for identified i2b2 database.
                        [default: ID_DB_ACCESS]
  add-result            add patient set result to query in deidentified i2b2
  <patients>            CSV file with patient_num column
  --deid-schema SCHEMA  schema with qt_tables where we will find
                        the dummy query (by name) and store the
                        new patient set
                        [default: BLUEHERONDATA]
  --deid-key NAME       name of environment variable to find
                        SQLAlchemy DB URL.
                        [default: DEID_DB_ACCESS]
  --cohort-table NAME   name of scratch table where in we can store the
                        cohort in the deid db [default: gpc_bc_consented]
  --debug
  --help

'''

import logging

from docopt import docopt
from pandas import read_sql, concat as df_concat

log = logging.getLogger(__name__)


def main(argv, environ, cwd, create_engine):
    '''See usage above.

    @param argv: CLI args a la sys.argv
    @param environ: process environment a la os.environ
    @param cwd: access to files a la pathlib (plus pd.read_csv)
    @param create_engine: access to databases by URL
                          a la sqlalchemy.create_engine
    '''
    cli = docopt(__doc__, argv=argv[1:])
    log.debug('cli: %s', cli)

    if cli['crosswalk']:
        consented = read_consented(cwd / cli['<consented>'])

        consented_mrn = mix_mrn(cwd / cli['<survey_order>'], consented)
        pmap = PatientMapping(db=create_engine(environ[cli['--id-key']]),
                              schema=cli['--id-schema'])
        log.info('Mapping %s MRNs (source: %s) to patient_num',
                 len(consented_mrn), cli['--mrn-source'])
        consented_crosswalk = pmap.by_mrn(consented_mrn,
                                          mrn_source=cli['--mrn-source'],
                                          aux_cols=['study_id', 'date_shift'])
        log.info('saving crosswalk (%s subjects) to %s',
                 len(consented_crosswalk), cli['<crosswalk>'])
        with (cwd / cli['<crosswalk>']).open('wb') as out:
            consented_crosswalk.to_csv(out, index=False)

    elif cli['patient-map']:
        pmap = PatientMapping(db=create_engine(environ[cli['--id-key']]),
                              schema=cli['--id-schema'])
        cohort_mrns = (cwd / cli['<cohort_mrns>']).read_csv()
        log.info('Mapping %s MRNs (source: %s) to patient_num',
                 len(cohort_mrns), cli['--mrn-source'])
        patient_nums = pmap.by_mrn(cohort_mrns,
                                   mrn_source=cli['--mrn-source'])
        log.info('saving %s patient_num results to %s',
                 len(patient_nums), cli['<patient_nums>'])
        with (cwd / cli['<patient_nums>']).open('wb') as out:
            patient_nums.to_csv(out, index=False)

    elif cli['add-result']:
        pat = (cwd / cli['<patients>']).read_csv()
        work = Workplace(db=create_engine(environ[cli['--deid-key']]),
                         schema=cli['--deid-schema'],
                         scratch=cli['--cohort-table'])
        pset = work.add_pset_result(
            cli['<dummy_query>'], pat)
        log.info('resulting patient set:\n%s', pset)


def read_consented(path):
    consented = path.read_csv()[['study_id', 'mrn']]
    log.info('consented.count():\n%s', consented.count())

    x = consented.sort_values('mrn')
    dups = x.mrn[x.mrn.duplicated()]
    if len(dups):
        log.error('duplicate MRNs:\n%s', x[x.mrn.isin(dups)])
        raise IOError
    return consented


def mix_mrn(path, consented):
    survey_order = _lower_cols(path.read_csv())[['order_id', 'mrn']]
    log.info('survey_order.count()\n%s',
             survey_order.count())
    consented_mrn = survey_order.merge(consented).sort_values('study_id')
    log.info('consented_mrn.count()\n%s',
             consented_mrn.count())
    return consented_mrn


class PatientMapping(object):
    def __init__(self, db, schema):
        def query(q, params):
            return read_sql(q.format(schema=schema), db, params=params)
        self.query = query

    def by_mrn(self, pat, mrn_source, aux_cols=[],
               max_list_size=256):
        '''
        @param max_list_size: maximum size of in (...) expression.

        ORA-01795: maximum number of expressions in a list is 1000
        '''
        cw_chunks = []
        for lo in range(0, len(pat), max_list_size):
            mrn_list_expr = ', '.join("'%d'" % n
                                      for n in pat.mrn[lo:lo + max_list_size])
            cw_chunks.append(self.query('''
                select distinct patient_num, to_number(patient_ide) mrn
                     , (select date_shift
                        from {schema}.patient_dimension pd
                        where pd.patient_num = pm.patient_num) date_shift
                from nightherondata.patient_mapping pm
                where pm.patient_ide_source = :mrn_source
                and pm.patient_ide in ({mrn_list})
                '''.replace('{mrn_list}', mrn_list_expr),
                                   params=dict(mrn_source=mrn_source)))
        crosswalk = df_concat(cw_chunks)
        log.debug('%s', pat.columns)
        log.debug('%s', crosswalk.columns)
        consented_crosswalk = pat.merge(crosswalk)[[
            'patient_num'] + aux_cols]
        log.info('len(consented_crosswalk): %s', len(consented_crosswalk))
        return consented_crosswalk


class Workplace(object):
    def __init__(self, db, schema, scratch):
        def query(q, params):
            return read_sql(q.format(schema=schema, scratch=scratch),
                            db, params=params)
        self.query = query

        def execute(sql, **params):
            return db.execute(sql.format(schema=schema,
                                         scratch=scratch), **params)
        self.execute = execute

        def save_data(df):
            log.info('creating %s', scratch)
            df.to_sql(scratch, db, if_exists='replace')
        self.save_data = save_data

    def lookup_query(self, query_name):
        detail = self.execute('''
        select qm.query_master_id, qi.query_instance_id
        from {schema}.qt_query_master qm
        join {schema}.qt_query_instance qi
          on qi.query_master_id = qm.query_master_id
        where qm.name = :query_name
        ''', query_name=query_name).first()
        if not detail:
            raise KeyError(query_name)
        return detail

    def add_pset_result(self, query_name, pat):
        qi = self.lookup_query(query_name)

        self.save_data(pat[['patient_num']])

        pset_id = self.execute('''
        select {schema}.QT_SQ_QRI_QRIID.nextval pset_id from dual
        ''').fetchone().pset_id

        self.execute('''
        insert into {schema}.qt_query_result_instance qri
          (result_instance_id, query_instance_id, result_type_id,
           set_size, real_set_size,
           start_date, end_date, delete_flag, status_type_id,
           description)

        select :pset_id, :qiid, 1
             , :set_size, :set_size
             , sysdate, sysdate, 'N', 3
             , 'Patient set for "' || :query_name || '"'
        from dual
        ''', pset_id=pset_id, qiid=qi.query_instance_id,
                     set_size=len(pat), query_name=query_name)

        self.execute('''
        insert into {schema}.qt_patient_set_collection
          (patient_set_coll_id, result_instance_id, set_index, patient_num)

        select
            {schema}.QT_SQ_QPR_PCID.nextval
          , :pset_id
          , bc."index" + 1
          , bc.patient_num
        from {scratch} bc
        ''', pset_id=pset_id)

        return self.query(
            '''
            select
              :pset_id patient_set_id,
              (select count(*) from {scratch}) size1,
              (select count(*) from {schema}.qt_patient_set_collection
               where result_instance_id = :pset_id) size2
            from dual
            ''', params=dict(pset_id=pset_id))


def _lower_cols(df):
    return df.rename(columns=dict((n, n.lower()) for n in df.keys()))


class PandasPath(object):
    '''pathlib API mixed with pandas I/O
    '''
    def __init__(self, path, ops):
        io_open, pathjoin, read_csv = ops
        self.open = lambda mode='r': io_open(path, mode=mode)
        self.read_csv = lambda: read_csv(path)
        self.pathjoin = lambda other: self.__class__(
            pathjoin(path, other), ops)

    def __div__(self, other):
        return self.pathjoin(other)


if __name__ == '__main__':
    def _script():
        from io import open as io_open
        from os import environ
        from os.path import join as pathjoin
        from sys import argv
        from pandas import read_csv
        from sqlalchemy import create_engine

        logging.basicConfig(level=logging.DEBUG if '--debug' in argv
                            else logging.INFO)
        main(argv, cwd=PandasPath('.', (io_open, pathjoin, read_csv)),
             environ=environ, create_engine=create_engine)

    _script()
