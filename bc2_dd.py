
# coding: utf-8

# In[ ]:

from xml.etree import ElementTree as ET
import re


# In[ ]:

import pandas as pd
dict(pandas=pd.__version__)


# In[ ]:

def file_access():
    from pathlib import Path
    return Path('.')
cwd = file_access()


# ## Babel DB Access

# In[ ]:

def db_access(key='BABEL_DB'):
    from os import getenv, environ
    from sqlalchemy import create_engine

    url = getenv(key)
    if not url:
        raise IOError(key)
    return create_engine(url)

babel_db = db_access()
babel_db.execute('select 1+1').fetchone()


# ## Data Dictionary

# ### i2b2 query: BC Phase 2e of Dec 17, 2015

# In[ ]:

bc295 = cwd / 'bc295_query_definition.xml'


# In[ ]:

def strip_counts(label):
    return re.sub(r'\[\d.*', '', label)
strip_counts('[AN000] ANTINEOPLASTICS [2,134,661 facts')


# In[ ]:

def item_ont(item_key):
    return item_key.split('\\')[4]

item_ont(r'\\i2b2_Medications\i2b2\Medications\RXAUI:3257')


# In[ ]:

with bc295.open() as s:
    qdef = ET.parse(s).getroot()

def e2d(e, keys):
    get = lambda ty, e: None if e is None else ty(e.text)
    return [(k, get(ty, e.find(k))) for (k, ty) in keys]

panel_cols = [('panel_number', int),
             ('invert', int),
             ('panel_date_from', str),
             ('panel_timing', str)]  # categorical
item_cols = [('hlevel', int),
             ('item_name', str),
             ('item_key', str),
             ('item_icon', str),  # categorical
             ('tooltip', str),
             ('class', str),
             ('item_is_synonym', bool)]
qitem = pd.DataFrame([dict(e2d(panel, panel_cols) + e2d(item, item_cols))
              for panel in qdef.iter('panel')
              for item in panel.iter('item')])


qitem['short_name'] = qitem.item_name.apply(strip_counts)
qitem['ont'] = qitem.item_key.apply(item_ont)

q_nice_cols = ['panel_number', 'hlevel', 'ont', 'tooltip', 'short_name']
qitem[q_nice_cols]


# In[ ]:

qitem[~qitem.ont.isin(['naaccr', 'Diagnoses', 'Procedures'])][q_nice_cols]


# In[ ]:

qitem.iloc[46]


# #### Medications: Antineoplastics, Hormones

# In[ ]:

qitem[qitem.ont == 'Medications'][q_nice_cols]


# In[ ]:

def gpc_std(item_key):
    return '\\GPC\\' + item_key.split('\\', 4)[-1]


# In[ ]:

med_term_paths = ', '.join("'%s'" % p
                           for p in qitem[qitem.ont == 'Medications'].item_key.apply(gpc_std))
print med_term_paths


# In[ ]:

pd.read_sql(r'''
select *
from i2b2metadata.gpc_terms
where c_fullname in ({paths})
limit 100
'''.format(paths=med_term_paths), babel_db)


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
'''.format(paths=med_term_paths)
med_va_class = pd.read_sql(sql, babel_db)
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
'''.format(paths=med_term_paths)
rx = pd.read_sql(sql, babel_db)
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


# ### Data Management Spreadsheet Dec 2015

# In[ ]:

# https://informatics.gpcnetwork.org/trac/Project/attachment/ticket/295/BC-MasterDataManagement.xlsx
# 2015-12-16T11:58:36-05:00
mdm = pd.read_excel('BC-MasterDataManagement.xlsx')
mdm.head()


# In[ ]:

mdm[(mdm.source != 'registry') & ~pd.isnull(mdm['Variable Name'])][['Variable Name']]


# In[ ]:

pd.DataFrame(dict(name=mdm['Variable Name'].unique()))

