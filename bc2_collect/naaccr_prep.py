
# coding: utf-8

# In[1]:

import pandas as pd


# In[2]:

import logging

log = logging.getLogger()

def show(x, label=None):
    return x


# In[18]:

def file_access():
    from pathlib import Path
    return Path('.')
cwd = file_access()


# In[34]:

bc_tumor_fields = pd.read_csv((cwd / 'bc_codebook_ddict.csv').open()).set_index('field_name')

naaccr_fields = (bc_tumor_fields[['field_label', 'text_validation_type_or_show_slider_number']]
                 .rename(columns=dict(text_validation_type_or_show_slider_number='validation')))
naaccr_fields['naaccr_item'] = naaccr_fields.index.str.extract(r'v\d{2,3}_(\d{3,4})_').astype('float32')
naaccr_fields = naaccr_fields[~pd.isnull(naaccr_fields.naaccr_item)]
naaccr_fields = naaccr_fields.reset_index().set_index('naaccr_item')
naaccr_fields.head()


# In[3]:

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


# In[4]:

t_item = pd.read_sql('''
select *
from naaccr.t_item
''', babel_db)
t_item.columns


# In[5]:

t_item = pd.read_sql('''
select cast("ItemNbr" as integer) itemnbr, "ItemName"
     , cast("ColumnStart" as integer) columnstart
     , cast("ColumnEnd" as integer) columnend
     , "Description"
from naaccr.t_item
where "ColumnStart" > ''
and "ColumnEnd" > ''
''', babel_db, index_col='itemnbr')
t_item.head()


# In[38]:

mrn_field = pd.DataFrame([dict(field_name='mrn')], index=[20])
mrn_field


# In[40]:

fields = naaccr_fields.append(mrn_field).join(t_item)
fields.head()


# In[32]:

naaccr_fixed = (cwd / 'site-data/naaccr_some.dat').open().readlines()
len(naaccr_fixed)


# In[51]:

def fixed_items(lines, fields, key_ix):
    def record(s, key, item_num, field_name, ty):
        val = None if s.replace(' ', '') == '' else s
        val = '%s-%s-%s' % (s[:4], s[4:6], s[6:]) if val and ty == 'date_ymd' else val
        return dict(key=key, item_num=item_num, value=val, field_name=field_name)

    key_field = fields.loc[key_ix]
    eav = pd.DataFrame([record(line[f.columnstart - 1:f.columnend],
                               line[key_field.columnstart - 1:key_field.columnend],
                               item_num, f.field_name, f.validation)
                        for line in lines
                        for item_num, f in fields.iterrows()])
    return eav.pivot(index='key', columns='field_name', values='value')

fixed_items(naaccr_fixed, fields, 20).head()

