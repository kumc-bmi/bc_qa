
# coding: utf-8

# # GPC Breast Cancer EMR and Tumor Registry Data Dictionary
# 
# context:
# 
#   - [ticket:382 distribute the query to the sites for the data for consented breast cancer survey patients](https://informatics.gpcnetwork.org/trac/Project/ticket/382)
#   - [GPC BC 2nd Data Pull Slides](https://docs.google.com/presentation/d/1LANts9zyDNyR3uPoArU04tsrxaC2Ao3rQyRk9JRPg9c/edit)

# ## Preface: PyData Scientific Python Tools
# 
# See also [PyData](http://pydata.org/).

# In[ ]:

# python standard library
from xml.etree import ElementTree as ET
import re


# In[ ]:

import pandas as pd
dict(pandas=pd.__version__)


# ## Record-per-tumor project
# 
# Rather than try to squish diagnoses, procedures, and medications into a one-record-per-tumor format,
# we're addressing them separately:
#    - Medication exposures project (below)
#    - all diagnoses and procedures (ticket #nnn)
# 
# But for the tumor registry data and a few other EMR fields we're using:
# 
#   - [GPC REDCap project 32: Breast Cancer Datamart](https://redcap.gpcnetwork.org/redcap_v6.11.5/index.php?pid=32)

# In[ ]:

def file_access():
    from pathlib import Path
    return Path('.')
cwd = file_access()


# In[ ]:

bc_tumor_fields = pd.read_csv((cwd / 'bc_codebook_ddict.csv').open()).set_index('field_name')

bc_tumor_fields['v_num'] = bc_tumor_fields.index.str.extract(r'v(\d{2,3})_').astype('int')
bc_tumor_fields['naaccr_item'] = bc_tumor_fields.index.str.extract(r'v\d{2,3}_(\d{3,4})_').astype('float32')

bc_tumor_fields[['form_name', 'field_type', 'field_label', 'field_note']].head()


# ### Tumor registry variables
# 
# Most of the variables come from the tumor registry:

# In[ ]:

len(bc_tumor_fields[~pd.isnull(bc_tumor_fields.naaccr_item)]), len(bc_tumor_fields)


# The first few are:

# In[ ]:

bc_tumor_fields[~pd.isnull(bc_tumor_fields.naaccr_item)][[0, 1, 2, 3]].head()


# Variables not from NAACCR are:

# In[ ]:

bc_tumor_fields[pd.isnull(bc_tumor_fields.naaccr_item)][[0, 1, 2, 3]]


# In[ ]:

# TODO: @ vis NI for deceased


# In[ ]:

# TODO: other vitals. and longitudinal for vitals


# ### NAACCR codes: check data dictionary vs. ontology on babel
# 
# Or data dictionary has standarized mappings from codes to labels:

# In[ ]:

def parse_choices(txt):
    return [tuple(item.split(', ', 1))
            for item in txt.split('\n')]

ddict_choices = pd.DataFrame([
        dict(choice_code=code, code_label=label,
            naaccr_item=field.naaccr_item,
            field_label=field.field_label)
        for (name, field) in bc_tumor_fields[~pd.isnull(bc_tumor_fields.naaccr_item)].iterrows()
        if not pd.isnull(field.select_choices_or_calculations)
        for (code, label) in parse_choices(field.select_choices_or_calculations)
    ],
            columns=['naaccr_item', 'field_label', 'choice_code', 'code_label'])

ddict_choices.head()


# In[ ]:

# Babel DB Access
def db_access(key='BABEL_DB'):
    from os import getenv, environ
    from sqlalchemy import create_engine

    url = getenv(key)
    if not url:
        raise IOError(key)
    return create_engine(url)

babel_db = db_access()
babel_db.execute('select 1+1').fetchone()


# ### Beware leading 0s: check code lengths
# 
# Earlier codebook drafts omitted leading 0s, complicating data import.

# In[ ]:

# NAACCR items, labels

t_item = pd.read_sql('''
-- "Description"
select "ItemNbr", "ItemName",  "SectionID", "FieldLength", "Format", "AllowValue"
from naaccr.t_item
''', babel_db)
t_item['item'] = t_item.ItemNbr.astype('int')
t_item = t_item.set_index('item')
t_item['FieldLength'] = t_item.FieldLength.astype('float64')
t_item.head()


# In[ ]:

# How long is each code in the REDCap data dictionary?
x = ddict_choices[['naaccr_item', 'field_label', 'choice_code']].copy()
x['code_len'] = x.choice_code.str.len()

# Any codes from REDCap whose lengths don't match the NAACCR t_item.FieldLength?
x = x.merge(t_item[['FieldLength']], left_on='naaccr_item', right_index=True, how='left')
x[x.code_len != x.FieldLength]


# In[ ]:

# If there are, halt and catch fire.
assert len(x[x.code_len != x.FieldLength]) == 0


# ### Compare GPC Ontology values vs. REDCap data dictionary values

# In[ ]:

def gpc_std(item_keys,
            c_table_cd='GPC'):
    '''GPC hasn't standardized c_table_cd; ignore it.
    '''
    return item_keys.apply(lambda k: '\\' + c_table_cd + '\\' + k.split('\\', 4)[-1])

def path_constraint(terms,
                    c_table_cd='GPC'):
    '''Build a SQL "in (...)" constraint from a dataframe of terms.
    '''
    paths = gpc_std(terms.item_key, c_table_cd=c_table_cd)
    params = dict(('param' + str(ix), path)
                  for (ix, path) in enumerate(paths))
    expr = ', '.join('%({0})s'.format(k) for k in params.keys())
    return expr, params


# Get metadata from babel for all concepts in the NAACCR ontology:

# In[ ]:

naaccr_relevant = pd.read_sql(r'''
  select c_hlevel, c_name, c_fullname, c_tooltip, c_basecode
  from i2b2metadata.heron_terms
  where c_fullname like '\i2b2\naaccr\S:%%' escape '@'
  order by c_fullname
''', babel_db)

len(naaccr_relevant)


# Clean up the code labels and extract item and section info from path, tooltip:

# In[ ]:

def strip_counts(label):
    return None if label is None else re.sub(r' \[[<\d].*', '', label)

[strip_counts(txt)
 for txt in ['[AN000] ANTINEOPLASTICS [2,134,661 facts',
             '07 [<10 facts]']]


# In[ ]:

map_option = lambda f: lambda x: None if not x else f(x)

# GPC doesn't standardize c_basecode,
# but this draws on heron_terms where the c_basecode pattern is known.
naaccr_relevant['naaccr_item'] = naaccr_relevant.c_basecode.apply(
    map_option(lambda c: int(c.split('|')[1].split(':')[0]))).astype('float64')
# Again, GPC doesn't standardize tooltips, but their structure is known for heron_terms.
naaccr_relevant['section'] = naaccr_relevant.c_tooltip.apply(
    map_option(lambda tip: ''.join(tip.split(' \\ ')[1:2])))
naaccr_relevant['item_name'] = naaccr_relevant.c_tooltip.apply(
    map_option(lambda tip: ''.join(tip.split(' \\ ')[2:3])))
naaccr_relevant['code_label'] = naaccr_relevant.c_name.apply(strip_counts)
naaccr_relevant.set_value(naaccr_relevant.c_hlevel < 4, 'code_label', None)

def naaccr_choice_codes(terms):
    return [
        (None if term.c_fullname is None or term.c_hlevel < 4 else
         term.c_fullname.split('\\')[int(term.c_hlevel + 1)].split(' ', 1)[0])
        for (_, term) in terms.iterrows()]

naaccr_relevant['choice_code'] = naaccr_choice_codes(naaccr_relevant)


# In[ ]:

naaccr_relevant[[
        'naaccr_item', 'section', 'item_name', 'choice_code', 'code_label']].head(8)


# ## Codes in REDCap but not in the ontology on babel

# In[ ]:

# First let's check that we have no NAACCR variables
# in the data dictionary that are not on babel.
bc_tumor_fields[
    ~pd.isnull(bc_tumor_fields.naaccr_item) &
    ~bc_tumor_fields.naaccr_item.isin(naaccr_relevant.naaccr_item.astype('float64'))
][['field_label', 'naaccr_item']]


# Some codes in REDCap seem to be newer codes that are perhaps not
# in the version of the NAAACCR ontology used in babel.
# 
# We're OK unless/until these codes show up in submitted data.

# In[ ]:

check1 = ddict_choices.set_index(['naaccr_item', 'choice_code']).join(
    naaccr_relevant.set_index(
        ['naaccr_item', 'choice_code'])[['section', 'c_basecode']],
    how='left', rsuffix='_db')

check1[pd.isnull(check1.c_basecode)]


# @@TODO: check for codes in babel but not in REDCap?

# ## Code in REDCap but not in the i2b2 query

# The i2b2 query **BC Phase 2e** of Dec 17, 2015 has over 100 terms from NAACCR plus several others:

# In[ ]:

bc295 = cwd / 'bc295_query_definition.xml'


# In[ ]:

def item_ont(item_key):
    part = item_key.split('\\')
    return part[5] if part[4] == 'naaccr' and part[5] == 'SEER Site' else part[4]

item_ont(r'\\i2b2_Medications\i2b2\Medications\RXAUI:3257')


# @@TODO: site-specific factors?

# In[ ]:

with bc295.open() as s:
    qdef = ET.parse(s).getroot()

def e2d(e, keys):
    get = lambda ty, e: None if e is None else ty(e.text)
    return [(k, get(ty, e.find(k))) for (k, ty) in keys]

#panel_cols = [('panel_number', int),
#             ('invert', int),
#             ('panel_date_from', str),
#             ('panel_timing', str)]  # categorical
item_cols = [('hlevel', int),
             ('item_name', str),
             ('item_key', str),
             #('item_icon', str),  # categorical
             ('tooltip', str),
             #('class', str),
             #('item_is_synonym', bool)
            ]
qitem = pd.DataFrame([dict(# e2d(panel, panel_cols) +
                           e2d(item, item_cols))
              # for panel in qdef.iter('panel')
              for item in qdef.iter('item')])


qitem['short_name'] = qitem.item_name.apply(strip_counts)
del qitem['item_name']
qitem = qitem.sort_values('item_key').drop_duplicates().reset_index(drop=True)
qitem['ont'] = qitem.item_key.apply(item_ont)

qitem.tail()


# In[ ]:

qitem.groupby('ont')[['item_key']].count()


# For the ones from NAACCR, let's pick out the item number:

# In[ ]:

qitem.set_value(qitem.ont == 'naaccr', 'naaccr_item', qitem.short_name.apply(lambda s: s[:4]))
qitem.naaccr_item = qitem.naaccr_item.astype('float64')

qitem[qitem.ont == 'naaccr'][['tooltip', 'short_name', 'naaccr_item']].head()


# The following are missing from the query!

# In[ ]:

bc_tumor_fields[
    ~pd.isnull(bc_tumor_fields.naaccr_item) &
    ~bc_tumor_fields.naaccr_item.isin(qitem.naaccr_item)
][['field_label', 'form_name', 'naaccr_item']]


# I'm looking for them in the ontology; what section are they in?

# In[ ]:

naaccr_relevant[naaccr_relevant.naaccr_item.isin([2830, 2840, 3270]) & pd.isnull(naaccr_relevant.choice_code)][['naaccr_item', 'section']]


# ## Demographics

# In[ ]:

bc_tumor_fields.groupby('form_name')[['field_label']].count()


# In[ ]:

bc_tumor_fields[(bc_tumor_fields.form_name == 'demographic') &
                # NAACCR fields are already done
                pd.isnull(bc_tumor_fields.naaccr_item)]
#bc2_ddict[bc2_ddict['Form Name'] == 'Demographics'][['Form Name', 'Field Label', 'Field Type']]


# demographics TODOs@@:
# 
#   - codes for gender, rase, ...
#   - codes from naaccr
#   - group naaccr race with EMR race?

# ## Vitals

# In[ ]:

bc2_ddict.set_value(qitem.ont == 'Visit Details', 'Form Name', 'Visit Vitals')
bc2_ddict[bc2_ddict['Form Name'] == 'Visit Vitals'][['Form Name', 'Field Label']]


# @@vitals todos:
#   - baseline, 1 year, two years
#   - field type, validation

# In[ ]:

x = pd.DataFrame(dict(form=bc2_ddict['Form Name'],
                      tooltip=qitem.tooltip,
                      name=qitem.short_name,
                     ont=qitem.ont),
                columns='ont form tooltip name'.split())
x[pd.isnull(x.form) & ~qitem.ont.isin(['Diagnoses', 'Procedures', 'Medications'])]


# ### @@other

# In[ ]:

qitem[~qitem.ont.isin(['naaccr', 'Diagnoses', 'Procedures'])][q_nice_cols]


# #### Medications: Antineoplastics, Hormones

# In[ ]:

qitem[qitem.ont == 'Medications'][q_nice_cols]


# In[ ]:

expr, params = path_constraint(qitem[qitem.ont == 'Medications'])
pd.read_sql(r'''
select *
from i2b2metadata.gpc_terms
where c_fullname in ({expr})
limit 100
'''.format(expr=expr), babel_db, params=params)


# In[ ]:

# Note %s have to be doubled in sqlalchemy API
sql = r'''
with va_top as (
  select *
  from i2b2metadata.gpc_terms
  where c_fullname in ({paths})
)
, va_class as (
  select sub.c_hlevel, sub.c_basecode, sub.c_name, sub.c_fullname
  from i2b2metadata.gpc_terms sub
  join va_top on sub.c_fullname like (va_top.c_fullname || '%%') escape '@'
  where sub.c_name like '[%%]%%'
)
select * from va_class
order by c_fullname
limit 100
'''.format(paths=expr)
med_va_class = pd.read_sql(sql, babel_db, params=params)
med_va_class['code'] = med_va_class.c_name.apply(lambda s: s.split('] ')[0][1:])
med_va_class.set_index('code')[['c_name']]


# In[ ]:

# Note %s have to be doubled in sqlalchemy API
sql = r'''
with va_top as (
  select *
  from i2b2metadata.gpc_terms
  where c_fullname in ({paths})
)
, va_class as (
  select sub.c_hlevel, sub.c_basecode, sub.c_name, sub.c_fullname
  from i2b2metadata.gpc_terms sub
  join va_top on sub.c_fullname like (va_top.c_fullname || '%%') escape '@'
  where sub.c_name like '[%%]%%'
)
, rx as (
  select sub.c_hlevel, sub.c_basecode, sub.c_name
       , substr(va_class.c_name, 2, 5) va_class_code
       , sub.c_fullname
  from i2b2metadata.gpc_terms sub
  join va_class
    on sub.c_fullname like (va_class.c_fullname || '%%') escape '@'
   and sub.c_hlevel = va_class.c_hlevel + 1
  where sub.c_name not like '[%%]%%'
)
select distinct * from rx
order by va_class_code, c_basecode
'''.format(paths=expr)
rx = pd.read_sql(sql, babel_db, params=params)
print 'distinct drugs:', len(rx)
print 'hlevels:', rx.c_hlevel.unique()
rx['rxcui'] = rx.c_basecode.apply(lambda s: s.split(':')[1])


# #### Unique Drugs: eliminating polyhierarchy

# In[ ]:

len(rx.rxcui), len(rx.rxcui.unique())


# In[ ]:

rx.sort_values('rxcui', inplace=True)
dup_cuis = rx[rx.rxcui.duplicated()].rxcui
rx[rx.rxcui.isin(dup_cuis)]


# In[ ]:

#rx1 = pd.DataFrame(dict(va_class_code=rx.groupby('rxcui').va_class_code.min()))
rx1 = rx.groupby('rxcui')[['va_class_code', 'c_name']].min()
print len(rx1)
rx1.head()


# In[ ]:

rx1['label'] = ['[{vc}] RXCUI:{cui} {drug}'.format(vc=drug.va_class_code, cui=rxcui, drug=drug.c_name)
                for (rxcui, drug) in rx1.iterrows()]
rx1.sort_values(['va_class_code', 'c_name'], inplace=True)
rx1.head(20)[['va_class_code', 'c_name', 'label']]


# ## Data Dictionary for REDCap

# In[ ]:

ddict_prototype = pd.read_csv((cwd / 'redcap_ddict_prototype.csv').open()).set_index(u'Variable / Field Name')
ddict_prototype.columns


# In[ ]:

def choices(series):
    return ' | '.join('{code}, {label}'.format(code=code, label=label)
               for (code, label) in series.iteritems())


# In[ ]:

med_ddict = ddict_prototype.copy()
med_ddict.set_value('va_class', u'Choices, Calculations, OR Slider Labels', choices(med_va_class.set_index('code').c_name))
med_ddict.set_value('rxcui', u'Choices, Calculations, OR Slider Labels', choices(rx1.label))

med_ddict.to_csv('med_exposure_ddict.csv')
med_ddict

