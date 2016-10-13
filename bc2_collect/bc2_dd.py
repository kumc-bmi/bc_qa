
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



# ## Code in REDCap but not in the i2b2 query

# The i2b2 query **BC Phase 2e** of Dec 17, 2015 has over 100 terms from NAACCR plus several others:

# In[ ]:

bc295 = cwd / 'bc295_query_definition.xml'


# In[ ]:

def item_ont(item_key):
    part = item_key.split('\\')
    return part[5] if part[4] == 'naaccr' and part[5] == 'SEER Site' else part[4]

item_ont(r'\\i2b2_Medications\i2b2\Medications\RXAUI:3257')



def strip_counts(label):
    return None if label is None else re.sub(r' \[[<\d].*', '', label)


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


# qitem['short_name'] = qitem.item_name.apply(strip_counts)
del qitem['item_name']
qitem = qitem.sort_values('item_key').drop_duplicates().reset_index(drop=True)
qitem['ont'] = qitem.item_key.apply(item_ont)

qitem.tail()


# In[ ]:

qitem.groupby('ont')[['item_key']].count()



# #### Medications: Antineoplastics, Hormones

# In[ ]:

#qitem[qitem.ont == 'Medications'][q_nice_cols]



# In[ ]:

# Note %s have to be doubled in sqlalchemy API
'''
with med_terms as (
select *
from unmc_terms
where c_fullname between '\i2b2\Medications\' and '\i2b2\Medications\zz'
)
, va_top as (
select *
from med_terms
where c_fullname in (
  '\i2b2\Medications\RXAUI:3257528\', -- [AN000] ANTINEOPLASTICS
  '\i2b2\Medications\RXAUI:3257701\') -- [HS000] HORMONES/SYNTHETICS/MODIFIERS
)
, va_class as (
  select sub.c_hlevel, sub.c_basecode, sub.c_name, sub.c_fullname
  from med_terms sub
  join va_top on sub.c_fullname between va_top.c_fullname and (va_top.c_fullname || 'zz')
  where sub.c_name like '[%%]%%'
)

select * from va_class
order by c_fullname
'''
med_va_class = pd.read_csv('va_class.csv')
med_va_class['code'] = med_va_class.c_name.apply(lambda s: s.split('] ')[0][1:])


unmc_meds_q = '''
with med_terms as (
select *
from unmc_terms
where c_fullname between '\i2b2\Medications\' and '\i2b2\Medications\zz'
)
, va_top as (
select *
from med_terms
where c_fullname in (
  '\i2b2\Medications\RXAUI:3257528\', -- [AN000] ANTINEOPLASTICS
  '\i2b2\Medications\RXAUI:3257701\') -- [HS000] HORMONES/SYNTHETICS/MODIFIERS
)
, va_class as (
  select sub.c_hlevel, sub.c_basecode, sub.c_name, sub.c_fullname
  from med_terms sub
  join va_top on sub.c_fullname between va_top.c_fullname and (va_top.c_fullname || 'zz')
  where sub.c_name like '[%%]%%'
)

, rx as (
  select sub.c_hlevel, sub.c_basecode, sub.c_name
       , substr(va_class.c_name, 2, 5) va_class_code
       , sub.c_fullname
  from med_terms sub
  join va_class
    on sub.c_fullname between va_class.c_fullname and (va_class.c_fullname || 'zz')
   and sub.c_hlevel = va_class.c_hlevel + 1
  where sub.c_name not like '[%%]%%'
)
select distinct * from rx
order by va_class_code, c_basecode

'''


rx = pd.read_csv('bc_meds.csv')
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

rx1['label'] = ['[{vc}] RXCUI:{cui} {drug}'.format(vc=drug.va_class_code, cui=rxcui,
                                                    drug=strip_counts(drug.c_name))
                for (rxcui, drug) in rx1.iterrows()]
rx1.sort_values(['va_class_code', 'c_name'], inplace=True)
rx1.head(20)[['va_class_code', 'c_name', 'label']]


# ## Data Dictionary for REDCap

# In[ ]:

ddict_prototype = pd.read_csv((cwd / 'med_exposure_ddict.csv').open()).set_index(u'Variable / Field Name')
ddict_prototype.columns


# In[ ]:

def choices(series):
    return ' | '.join('{code}, {label}'.format(code=code, label=label)
               for (code, label) in series.iteritems())


# In[ ]:

med_ddict = ddict_prototype.copy()
#med_ddict.set_value('va_class', u'Choices, Calculations, OR Slider Labels', choices(med_va_class.set_index('code').c_name))
med_ddict.set_value('rxcui', u'Choices, Calculations, OR Slider Labels', choices(rx1.label))

med_ddict.to_csv('med_exposure_ddict.csv')
med_ddict

with (cwd / 'rx_choices.txt').open('wb') as out:
    for code, label in rx1.label.iteritems():
        print >>out, '{code}, {label}'.format(code=code, label=label)
