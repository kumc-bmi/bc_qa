
# coding: utf-8

# In[ ]:

from xml.etree import ElementTree as ET
import re


# In[ ]:

import pandas as pd
dict(pandas=pd.__version__)


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

# ### i2b2 query

# In[ ]:

def file_access():
    from pathlib import Path
    return Path('.')
cwd = file_access()


# In[ ]:

def strip_counts(label):
    return re.sub(r'\[\d.*', '', label)
strip_counts('[AN000] ANTINEOPLASTICS [2,134,661 facts')


# In[ ]:

def item_ont(item_key):
    return item_key.split('\\')[4]

item_ont(r'\\i2b2_Medications\i2b2\Medications\RXAUI:3257')


# In[ ]:

with (cwd / 'bc295_query_definition.xml').open() as s:
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

qitem[['panel_number', 'hlevel', 'ont', 'tooltip', 'short_name']]


# In[ ]:

qitem[~qitem.ont.isin(['naaccr', 'Diagnoses', 'Procedures'])][['panel_number', 'hlevel', 'ont', 'tooltip', 'short_name']]


# In[ ]:

qitem.iloc[46]


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

