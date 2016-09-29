
# coding: utf-8

# # EMR and Registry Data for Breast Cancer Survey Participants
# 
# Refs:
#   - [GPC BC 2nd Data Pull Slides](https://docs.google.com/presentation/d/1LANts9zyDNyR3uPoArU04tsrxaC2Ao3rQyRk9JRPg9c/edit)
#   - [Data Import Tool](https://redcap.gpcnetwork.org/redcap_v6.11.5/DataImport/index.php?pid=32) for per-tumor project 32.
#   - [Data Import Tool](https://redcap.gpcnetwork.org/redcap_v6.11.5/DataImport/index.php?pid=64) for per-medication-exposure project 64.
# 
# 
# - [#297 collect breast cancer data from GPC sites for linking with surveys, multi-signal analysis](https://informatics.gpcnetwork.org/trac/Project/ticket/297)
#   - [ticket:382 distribute the query to the sites for the data for consented breast cancer survey patients](https://informatics.gpcnetwork.org/trac/Project/ticket/382)

# ## Adjust filenames and settings to suit

# In[ ]:

site_access_group = '15'

builder_filename = 'site-data/bc-g297.db'
crosswalk_filename = 'site-data/consented_crosswalk.csv'

per_tumor_out = 'site-data/per-tumor.csv'
per_med_exp_out = 'site-data/per-medication-exposure.csv'

chunk_size = 2000  # output will be broken into files of at most this many records


# In[ ]:

tumor_import_template_filename = 'BreastCancerTumorDatamart_ImportTemplate_2016-09-28.csv'
med_import_template_filename = 'GPCBreastCancerMediationExposu_ImportTemplate_2016-09-29.csv'


# ## Preface: PyData Scientific Python Tools
# 
# See also [PyData](http://pydata.org/).

# In[ ]:

import pandas as pd

dict(pandas=pd.__version__)


# ### Mix crosswalk into builder file

# In[ ]:

def builder_file_access(name):
    from sqlite3 import connect
    return connect(name)

bc_data = builder_file_access(builder_filename)

pd.read_sql('select count(*) from patient_dimension', bc_data)


# In[ ]:

pd.read_sql('select pset, name from job', bc_data)


# In[ ]:

consented_crosswalk = pd.read_csv(crosswalk_filename)
consented_crosswalk.date_shift = pd.to_timedelta(consented_crosswalk.date_shift, unit='day')
consented_crosswalk.head()


# In[ ]:

consented_crosswalk[['study_id', 'patient_num']].to_sql('consented_crosswalk', bc_data, if_exists='replace')


# ### Data Dictionary Fields and NAACCR Item Numbers

# In[ ]:

# comma at end of header line causes goofy Unnamed: NNN column
def fix_template(t):
    return pd.DataFrame(
        t, columns=[c for c in t.columns
                    if not c.startswith('Unnamed: ')])
    
import_template = fix_template(pd.read_csv(tumor_import_template_filename))

if len(import_template.columns) != 141:
    raise ValueError('expected 125 columns; got: %s' % len(import_template.columns))

print "First few fields..."
import_template.columns[:10]


# For NAACCR fields, extract the NAACCR item number from the variable name:

# In[ ]:

import re

naaccr_field = pd.DataFrame(dict(field_name=name, item=int(name.split('_', 3)[1]))
            for name in import_template.columns
            if re.match('v\d{2,3}_\d{3,4}_', name)).sort_values('field_name').set_index('item')
print len(naaccr_field)
naaccr_field.head()


# In[ ]:

# @@TODO: Seer site summary


# In[ ]:

# @@TODO: vital status: @ -> NI


# ## NAACCR Coded Data

# In[ ]:

# We assume HERON-style concept_cd, not just GPC paths.

naaccr_coded_eav = pd.read_sql('''
select * from (
   select distinct patient_num, encounter_num, concept_cd
        , 0 + substr(concept_cd, length('NAACCR|_')) item
        , substr(concept_cd, instr(concept_cd, ':') + 1) code
   from observation_fact
   where concept_cd like 'NAACCR|%'
) where code > ''
''', bc_data)

print len(naaccr_coded_eav), len(naaccr_coded_eav[['encounter_num', 'item']].drop_duplicates())
naaccr_coded_eav.head()


# In[ ]:

# TODO: why no data for this item at KUMC?
# 746 RX HOSP--SURG SITE 98-02
naaccr_coded_eav[naaccr_coded_eav.item == 746]


# Now pivot the data:

# In[ ]:

naaccr_coded = (naaccr_coded_eav
                .merge(naaccr_field, left_on='item', right_index=True)
                .pivot(index='encounter_num', columns='field_name', values='code'))
print "coded data on", len(naaccr_coded), "tumors"
naaccr_coded.head()


# ## NAACCR Date fields, unshifted

# In[ ]:

from datetime import timedelta


# In[ ]:

naaccr_dated_eav = pd.read_sql('''
   select distinct patient_num, encounter_num, start_date date_deid
        , 0 + substr(concept_cd, length('NAACCR|_')) item
   from observation_fact
   where concept_cd like 'NAACCR|%:'
''', bc_data, parse_dates=['date_deid'])
naaccr_dated_eav = naaccr_dated_eav.merge(consented_crosswalk[['patient_num', 'date_shift']], how='left')
naaccr_dated_eav['date'] = naaccr_dated_eav.date_deid - naaccr_dated_eav.date_shift
naaccr_dated_eav.head()


# In[ ]:

naaccr_dated = (naaccr_dated_eav
                .merge(naaccr_field, left_on='item', right_index=True)
                .pivot(index='encounter_num', columns='field_name', values='date'))
print "date fields for", len(naaccr_dated), "tumors"
naaccr_dated.head()


# ## All NAACCR fields: coded fields + date fields

# In[ ]:

naaccr_records = naaccr_coded.join(naaccr_dated)
print len(naaccr_records)
naaccr_records.head()


# ## Per-patient fields: language, consent, SSA vital status

# In[ ]:

# remember to double \\s in sqlite string literals
pd.read_sql('''
select v.id, v.concept_path, v.name
from variable v
where concept_path not like '\\i2b2\\naaccr\\%'
and concept_path not like '\\i2b2\\Diagnoses\\%'
and concept_path not like '\\i2b2\\Procedures\\%'
and concept_path not like '\\i2b2\\Medications\\%'
and concept_path not like '\\i2b2\\Visit Details\\Vitals\\%'
order by concept_path
''', bc_data, index_col='id')


# In[ ]:

from io import StringIO

# EMR Gender, Race are ignored.
emr_dem_terms=pd.read_csv(StringIO(ur'''
concept_path,field_name
\i2b2\Demographics\Gender\,vxx_gender
\i2b2\Demographics\Language\,v52_language
\i2b2\Demographics\Marital Status\,vxx_maritalstatus
\i2b2\Demographics\Race\,vxx_race
\i2b2\Demographics\Vital Status\Deceased per SSA\,v53_deceased_per_ssa
'''.strip()))

emr_dem_terms


# In[ ]:

emr_dem_terms.to_sql('emr_dem_terms', bc_data, if_exists='replace')
dem_eav = pd.read_sql(
'''
select obs.patient_num, t.field_name, group_concat(substr(cd.concept_cd, instr(cd.concept_cd, ':') + 1)) code
from emr_dem_terms t
join concept_dimension cd on cd.concept_path like (t.concept_path || '%')
join observation_fact obs on obs.concept_cd = cd.concept_cd
group by obs.patient_num, t.field_name
''', bc_data)
dem_eav
dem = pd.DataFrame(dem_eav.pivot(index='patient_num', columns='field_name', values='code'),
                   columns=emr_dem_terms.field_name)
dem.head()


# In[ ]:

pat = pd.read_sql('''
   select distinct cw.patient_num, cw.study_id v01_studyid, 1 v02_breastsurvey, 1 v03_medrecordconsent
   from consented_crosswalk cw
''', bc_data, index_col='patient_num')
pat = pat.join(dem[['v52_language', 'v53_deceased_per_ssa']], how='left')
#pat = pat.set_index('v01_studyid')
print len(pat)
pat.head()


# ## Vitals - baseline, 1yr, 2yrs

# In[ ]:

emr_numeric_eav=pd.read_sql(
r'''
with
delta as (
  select 0 yr union all
  select 1 union all
  select 2
)
, direction as (
  select -1 sign union all
  select 1
)
, dx as (
  select distinct patient_num, encounter_num, start_date
  from observation_fact f
  where f.concept_cd = 'NAACCR|390:'
)
, obs as (
  select distinct patient_num, start_date, nval_num
  from observation_fact obs
  join concept_dimension cd on cd.concept_cd = obs.concept_cd
  where cd.concept_path in (?)
)
, event as (
  select dx.patient_num, dx.encounter_num, dx.start_date dx_date
       , datetime(dx.start_date, delta.yr || ' years') event_date
       , obs.start_date obs_date, obs.nval_num
       , delta.yr, sign
  from dx
  join obs on obs.patient_num = dx.patient_num
  cross join delta
  cross join direction
)
, candidate as (
  select encounter_num, patient_num, dx_date, event_date, yr, sign
       , obs_date, nval_num
       , julianday(obs_date) - julianday(event_date) delta
  from event
)
, winner as (
  select encounter_num, patient_num, dx_date, event_date, yr, sign, min(abs(delta)) delta
  from candidate
  where abs(delta) < 365 and delta * sign > 0
  group by encounter_num, dx_date, yr, sign
)
select winner.*
    , (select nval_num from candidate
       where candidate.delta = winner.delta * sign
       and candidate.encounter_num = winner.encounter_num
       and candidate.yr = winner.yr
       and candidate.sign = winner.sign) nval_num
    , (select obs_date from candidate
       where candidate.delta = winner.delta * sign
       and candidate.encounter_num = winner.encounter_num
       and candidate.yr = winner.yr
       and candidate.sign = winner.sign) obs_date_deid
    , ('v' || (? + winner.yr * 4 + ((winner.sign + 1))) || '_' || ? 
         || case when winner.sign > 0 then '_post_' else '_pre_' end
         || winner.yr || 'yr')  field_name
from winner
order by encounter_num, yr, sign
''', bc_data, params=[r'\i2b2\Visit Details\Vitals\BMI\ '[:-1], 116, 'bmi'],
    parse_dates=['dx_date', 'event_date', 'obs_date_deid'])

emr_numeric_eav = emr_numeric_eav.merge(consented_crosswalk[['patient_num', 'date_shift']], how='left')
emr_numeric_eav['obs_date'] = emr_numeric_eav.obs_date_deid - emr_numeric_eav.date_shift

emr_numeric_eav.head(20)


# In[ ]:

vital_num = emr_numeric_eav.pivot(index='encounter_num', columns='field_name', values='nval_num')
vital_date = emr_numeric_eav.pivot(index='encounter_num', columns='field_name', values='obs_date')

# v118_bmi_post_0yr -> v117_bmi_post_0yr_date
vital_date = vital_date.rename(
    columns=dict((n, 'v%d_%s_date' % (int(v) + 1, rest))
     for n in vital_num.columns
     for (v, rest) in [n[1:].split('_', 1)]))

vital = vital_num.join(vital_date)
vital = pd.DataFrame(vital, columns=sorted(vital.columns))
vital.describe()


# In[ ]:

x = pd.DataFrame(dict(name=vital.columns))
x['label'] = x.name.str.replace(r'^v\d+_', '').str.replace('bmi', 'BMI').str.replace('_', ' ')
x


# ## Full record: Tumor ID, tumor data, Patient data, vitals

# In[ ]:

tumor = pd.read_sql('''
  select distinct obs.encounter_num, cw.study_id v01_studyid
  from observation_fact obs
  left join consented_crosswalk cw on cw.patient_num = obs.patient_num
  where concept_cd like 'NAACCR|%'
''', bc_data).set_index('encounter_num')

tumor = tumor.join(naaccr_records[['v15_0380_sequence_numbercentral', 'v16_0560_sequence_numberhospital']])
tumor['v00_tumorid'] = [str(r.v01_studyid) + ':' + 
                  (r.v15_0380_sequence_numbercentral if r.v15_0380_sequence_numbercentral > '' else
                   r.v16_0560_sequence_numberhospital if r.v16_0560_sequence_numberhospital > '' else 'XX')
                 for _, r in tumor.iterrows()]
tumor = (tumor[['v00_tumorid', 'v01_studyid']]
         .join(naaccr_records, how='left')
         .join(vital, how='left')
         .merge(pat, on='v01_studyid', how='left'))
print len(tumor)
tumor.head()

#admin = pd.read_sql(admin_q, bc_data)

#admin


# In[ ]:

data = pd.DataFrame(tumor, columns=import_template.columns)
data['redcap_data_access_group'] = site_access_group

data['admin_complete'] = 2

data[['v00_tumorid', 'v01_studyid', 'v02_breastsurvey', 'v03_medrecordconsent', 'admin_complete']].head()


# In[ ]:

data.set_value(~pd.isnull(data.v20_0490_diagnostic_confirmation), 'test_complete', '2')
data[['v20_0490_diagnostic_confirmation', 'test_complete']].describe()


# In[ ]:

data.set_value(~pd.isnull(data.v52_language), 'demographic_complete', '2')
data[['v52_language', 'demographic_complete']].describe()


# In[ ]:

data.to_csv(per_tumor_out, index=False)


# In[ ]:

set(import_template.columns) - set(tumor.columns)


# In[ ]:

#@@ TODO: reconcile query vs. data dictionary
d = set(naaccr_coded_eav.item.append(naaccr_dated_eav.item))
dd = set(naaccr_field.index)
dict(
    data_item_qty=len(d),
    ddict_qty=len(dd),
    both=len(d & dd),
    data_only=d - dd,
    ddict_only=dd - d
)


# ## Medication Exposures

# In[ ]:

def parent_path(p):
    return '\\'.join(p.split('\\')[:-2]) + '\\'

parent_path('\\a\\b\\c\\')


# In[ ]:

med_term = pd.read_sql(
'''
with med_var as (
  select v.id, v.concept_path, v.name
  from variable v
  where concept_path like '\i2b2\Medications\%' escape '@'
)

select sub.concept_path concept_path, sub.concept_cd concept_cd, sub.name_char name_char
from concept_dimension sub
join med_var on sub.concept_path like (med_var.concept_path || '%%') escape '@' 
''', bc_data)
print len(med_term)
med_term['parent_path'] = med_term.concept_path.apply(parent_path)
med_term.head()


# In[ ]:

va_class = med_term[med_term.name_char.str.match(r'^\[.....\]')]
va_class.head()


# In[ ]:

rx = med_term.copy()
rx = rx[rx.concept_path.apply(parent_path).isin(va_class.concept_path)  # directly under a VA class
        & ~rx.concept_path.isin(va_class.concept_path) # but not itself a VA class
        & rx.concept_cd.str.match('^RXCUI:')]
rx['rxcui'] = rx.concept_cd.str.slice(len('RXCUI:'))
rx.set_index('rxcui', inplace=True)
print len(rx), len(rx.index.unique())
rx[rx.index.isin(
        rx[rx.index.duplicated()].index)]


# In[ ]:

def is_ancestor(pa, pd):
    return pd[:len(pa)] == pa

def ancestors(anc, desc):
    anc = anc.copy()
    anc['key'] = 1
    anc['path_length'] = anc.concept_path.str.len()
    desc = desc.copy()
    desc['key'] = 1
    desc['path_length'] = desc.concept_path.str.len()
    cross = anc.merge(desc, on='key', suffixes=('_a', ''))
    cross = cross[cross.path_length >= cross.path_length_a]
    cross['flag'] = [is_ancestor(c.concept_path_a, c.concept_path)
            for (_, c) in cross.iterrows()]
    return cross[cross.flag]

x = ancestors(rx.reset_index(), med_term[['concept_path', 'concept_cd', 'name_char']][:20])
x[['rxcui', 'name_char_a', 'name_char', 'concept_cd']].head()


# In[ ]:

# TODO: consider modifier_cd?
med_obs = pd.read_sql('''
select patient_num
     , start_date
     , instance_num
     , min(concept_cd) concept_cd
     , max(end_date) end_date
from observation_fact
group by instance_num, patient_num, start_date
order by patient_num, start_date
''', bc_data, parse_dates=['start_date', 'end_date']).drop_duplicates()
med_obs = med_obs[med_obs.concept_cd.isin(med_term.concept_cd)]
print len(med_obs)
med_obs.head()


# In[ ]:

med_code = med_term[med_term.concept_cd.isin(med_obs.concept_cd)].copy()
print len(med_code), len(med_code.concept_cd.unique())
x = ancestors(rx.reset_index()[['rxcui', 'name_char', 'concept_path']],
              med_code[['concept_cd', 'name_char', 'concept_path']])
med_code = med_code.merge(x.rename(columns=dict(name_char_a='drug_name')), how='left')[[
            'concept_cd', 'name_char', 'rxcui', 'drug_name']]
med_code = med_code.groupby('concept_cd')[['name_char', 'rxcui', 'drug_name']].min().reset_index()
print len(med_code), len(med_code.concept_cd.unique())
med_code.head()


# In[ ]:

med_exp = med_obs.merge(med_code, on='concept_cd')
med_exp = med_exp.merge(consented_crosswalk[['patient_num', 'study_id', 'date_shift']])

med_exp['raw_med_name'] = med_exp['name_char']
med_exp['exposure_start'] = (med_exp.start_date - med_exp.date_shift).dt.strftime('%Y-%m-%d %H:%M')
med_exp['exposure_end']   = (med_exp.end_date   - med_exp.date_shift).dt.strftime('%Y-%m-%d %H:%M')

med_exp['record_id'] = ['{pat}_{start}_{instance}'.format(
        pat=e.study_id, start=e.exposure_start, instance=e.instance_num)
                       for (_, e) in med_exp.iterrows()]
med_exp['medication_exposure_complete'] = '2'

med_exp['redcap_data_access_group'] = site_access_group

med_import_template = fix_template(pd.read_csv(med_import_template_filename))
med_exp = pd.DataFrame(med_exp, columns=med_import_template.columns)
print len(med_exp), len(med_exp.record_id.unique())
assert len(med_exp) == len(med_exp.record_id.unique())
med_exp.head()


# In[ ]:

from sys import stderr

def save_in_chunks(df, fn, chunk_size):
    if len(df) <= chunk_size:
        df.to_csv(fn, index=False)
        return

    base, ext = per_med_exp_out.split('.')
    digits = len(str(len(df)))
    for chunk in range(0, len(med_exp), chunk_size):
        fn = '%s-%0*d.%s' % (base, digits, chunk, ext)
        print >>stderr, 'writing to', fn
        df[chunk:chunk + chunk_size].to_csv(fn, index=False)

save_in_chunks(med_exp, per_med_exp_out, chunk_size)

