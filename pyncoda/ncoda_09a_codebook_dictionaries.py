"""
Codebook variable dictionaries for the linkage-era products.

Serves the codebook goal in issue #140: PDF codebooks for the person and
housing products, built with pypdfcodebook. The two existing datastructures
(prec_v300_DataStructure, incore_v1_HUA_DataStructure) document the classic
columns; this module supplements the variables added since - the vintage-true
householder names, family type, disability, the age band tables, and the
building join columns - and assembles a datastructure for exactly the columns
a given dataframe carries.

An undocumented column is reported loudly, never skipped silently: a codebook
that quietly omits a variable is the same disease as a validation that passes
on an empty set.

Band definitions restate the dictionaries in acg_02c_agefunctions,
acg_00h_disability_ACS5yr2022 and the group quarters tables; the sources are
named beside each so a future change there is traceable here.
"""

from pyncoda.ncoda_00f_hua_structure import incore_v1_HUA_DataStructure
from pyncoda.CommunitySourceData.api_census_gov.acg_00g_prec_datastructure \
    import prec_v300_DataStructure


def _bands(pairs):
    """{code: 'X to Y years'} from (code, lo, hi) triples."""
    out = {}
    for code, lo, hi in pairs:
        if lo == hi:
            label = f'{lo} years'
        elif hi >= 110:
            label = f'{lo} years and over'
        elif lo == 0:
            label = f'Under {hi + 1} years'
        else:
            label = f'{lo} to {hi} years'
        out[code] = label
    return out


# Age band tables, restated from their defining modules.
AGEGROUP_P12 = _bands([(1, 0, 4), (2, 5, 9), (3, 10, 14), (4, 15, 17),
                       (5, 18, 19), (6, 20, 20), (7, 21, 21), (8, 22, 24),
                       (9, 25, 29), (10, 30, 34), (11, 35, 39), (12, 40, 44),
                       (13, 45, 49), (14, 50, 54), (15, 55, 59), (16, 60, 61),
                       (17, 62, 64), (18, 65, 66), (19, 67, 69), (20, 70, 74),
                       (21, 75, 79), (22, 80, 84), (23, 85, 110)])
AGEGROUP_H17 = _bands([(1, 15, 24), (2, 25, 34), (3, 35, 44), (4, 45, 54),
                       (5, 55, 59), (6, 60, 64), (7, 65, 74), (8, 75, 84),
                       (9, 85, 110)])
AGEGROUP_H18 = _bands([(1, 15, 34), (2, 35, 64), (3, 65, 110)])
AGEGROUP_P43 = _bands([(1, 0, 17), (2, 18, 64), (3, 65, 110)])
AGEGROUP_B18101 = _bands([(1, 0, 4), (2, 5, 17), (3, 18, 34), (4, 35, 64),
                          (5, 65, 74), (6, 75, 110)])

FAMILYTYPE_CATEGORIES = {
    1: 'Married-couple family',
    2: 'Other family: male householder, no spouse present',
    3: 'Other family: female householder, no spouse present',
    4: 'Nonfamily household: male householder',
    5: 'Nonfamily household: female householder',
    -999: 'Not determined (vacant unit, group quarters, or not reached by the merge)',
}

SEX_CATEGORIES = {1: 'Male', 2: 'Female'}
NOT_SET = 'Value -999 means not set: the merge holds it as a matchable placeholder.'


def _entry(label, analysis_unit, measure_unit, datatype='Int', pytype='int',
           notes='', categories=None):
    e = {'label': label, 'DataType': datatype, 'pyType': pytype,
         'AnalysisUnit': analysis_unit, 'MeasureUnit': measure_unit,
         'notes': notes}
    if categories is not None:
        e['categorical'] = 'Categorical'
        e['categories_dict'] = categories
        # pypdfcodebook renders the category table only when pyType is the
        # literal string 'category' (the 'categorical' key is descriptive).
        e['pyType'] = 'category'
    return e


def supplement_entries(basevintage='2020'):
    """
    Entries for every variable the base datastructures do not cover, for one
    vintage. Column names that differ by vintage (the vintage-true householder
    names, geography, place names) are emitted under the names that vintage's
    files actually carry.
    """

    v = str(basevintage)
    yr = v[2:]
    person, unit = 'Person', 'Housing unit'

    fine_name = 'agegroupH13' if v == '2020' else 'agegroupH17'
    fine_src = 'H13' if v == '2020' else 'H17'
    coarse_name = 'agegroupH14' if v == '2020' else 'agegroupH18'
    coarse_src = 'H14' if v == '2020' else 'H18'
    sex_name = 'sexH14' if v == '2020' else 'sexH18'
    famtype_name = 'familytypeH14' if v == '2020' else 'familytypeH18'

    entries = {
        # --- person-side ------------------------------------------------
        'pernum': _entry(
            'Person number within housing unit', person, 'Person',
            notes='1 is the householder; 2 may be the spouse in family '
                  'households; higher numbers are other members. Assigned '
                  'when housing units are expanded into person slots.'),
        'randageP12': _entry(
            'Random single-year age (P12 bands)', person, 'Years',
            notes='Drawn uniformly within the person\'s P12 age band, '
                  'seeded for reproducibility.'),
        'agegroupP12': _entry(
            'Age group (Census table P12)', person, 'Person',
            categories=AGEGROUP_P12),
        'agegroupB18101': _entry(
            'Age group (ACS table B18101, disability universe)', person,
            'Person', categories=AGEGROUP_B18101),
        'disability': _entry(
            'Disability status (ACS B18101)', person, 'Person',
            categories={1: 'With a disability', 0: 'No disability',
                        -999: 'Not determined'},
            notes='Merged from ACS 5-year B18101 at tract level. ' + NOT_SET),
        'agegroupP43': _entry(
            'Group quarters age group (2010 table P43 bands, both vintages)',
            person, 'Person', categories=AGEGROUP_P43,
            notes='Only group quarters residents carry this band; the 2020 '
                  'source table is P18, which uses the same bands. ' + NOT_SET),
        'agegroupH17': _entry(
            'Householder age band carried onto the person slot (H17 bands)',
            person, 'Person', categories=AGEGROUP_H17, notes=NOT_SET),
        'agegroupH18': _entry(
            'Householder age band carried onto the person slot (H18 bands)',
            person, 'Person', categories=AGEGROUP_H18, notes=NOT_SET),
        'child': _entry(
            'Assumed child indicator', person, 'Person',
            categories={1: 'Assumed child of householder',
                        -999: 'Not an assumed child / not set'},
            notes='From household structure inference: in family households '
                  'person 3 onward, and person 2 onward for single parents.'),
        # --- geography and building join --------------------------------
        f'Block{v}': _entry(
            f'{v} Census block ID (numeric)', 'Geographic unit', unit,
            datatype='Float', pytype='float',
            notes='Numeric form; leading zeros are absent. Use the string '
                  'form for joins.'),
        f'Block{v}str': _entry(
            f'{v} Census block ID (string, B-prefixed)', 'Geographic unit',
            unit, datatype='String', pytype='object',
            notes='The letter B plus the 15-digit block GEOID, so '
                  'spreadsheets cannot strip leading zeros.'),
        f'County{v}': _entry(
            f'{v} county FIPS', 'Geographic unit', unit,
            datatype='Float', pytype='float'),
        f'Tract{v}': _entry(
            f'{v} Census tract ID', 'Geographic unit', unit,
            datatype='Float', pytype='float'),
        f'placeNAME{yr}': _entry(
            f'{v} Census place name', 'Geographic unit', unit,
            datatype='String', pytype='object',
            notes='Incorporated place or Census Designated Place containing '
                  'the block; blank outside any place.'),
        'fd_id_bid': _entry(
            'Building ID (National Structures Inventory)', 'Building',
            'Building', datatype='String', pytype='object',
            notes='Foreign key to the NSI building inventory. The sentinel '
                  '"missing building id" marks units that matched no '
                  'building in the allocation.'),
        'huestimate': _entry(
            'Estimated housing units in the building', 'Building', unit),
        'x': _entry('Longitude of the building point', 'Building', 'Degrees',
                    datatype='Float', pytype='float'),
        'y': _entry('Latitude of the building point', 'Building', 'Degrees',
                    datatype='Float', pytype='float'),
        'geometry': _entry(
            'Building point geometry (WKT)', 'Building', 'Point',
            datatype='String', pytype='object'),
        # --- housing-side additions from the householder step ------------
        fine_name: _entry(
            f'Householder age band (Census table {fine_src})', unit, unit,
            categories=AGEGROUP_H17, notes=NOT_SET),
        coarse_name: _entry(
            f'Householder age band (Census table {coarse_src})', unit, unit,
            categories=AGEGROUP_H18, notes=NOT_SET),
        sex_name: _entry(
            f'Householder sex (Census table {coarse_src})', unit, unit,
            categories={**SEX_CATEGORIES,
                        -999: 'Married-couple family (table reports no '
                              'single householder sex)'},
            notes='For married-couple families the source table does not '
                  'report a householder sex, so -999 is a meaningful '
                  'category here, not a missing value.'),
        famtype_name: _entry(
            f'Household family type (Census table {coarse_src})', unit, unit,
            categories=FAMILYTYPE_CATEGORIES),
        'randincomeB19101': _entry(
            'Random household income (ACS B19001/B19101)', unit, 'Dollars',
            datatype='Float', pytype='float',
            notes='Drawn within the household\'s income bin, seeded for '
                  'reproducibility.'),
        'poverty': _entry(
            'Below poverty threshold indicator', unit, unit,
            categories={1: 'Household income below the poverty threshold',
                        0: 'At or above the poverty threshold',
                        -999: 'Not determined'}),
    }

    if v == '2020':
        entries['ageP18'] = _entry(
            'Group quarters age band (Census table P18)', unit, unit,
            categories=AGEGROUP_P43,
            notes='Carried by group quarters units from the 2020 P18 pull; '
                  'the bands match the 2010 P43 table. ' + NOT_SET)
        entries['sexP18'] = _entry(
            'Group quarters resident sex (Census table P18)', unit, unit,
            categories=SEX_CATEGORIES, notes=NOT_SET)

    # Random-merge audit columns share one explanation.
    flag_note = ('Random merge working column, retained for audit: it '
                 'records the round in which the row was matched or set '
                 'aside. Not an analysis variable.')
    for flag in ('agegroupP43_flagsetrm', f'agegroupP43_Block{v}_flagsetrm',
                 'huid_flagsetrm', f'huid_Block{v}_flagsetrm'):
        entries[flag] = _entry('Random merge audit flag', person, 'Row',
                               datatype='Float', pytype='float',
                               notes=flag_note)
    return entries


def build_datastructure(df, basevintage='2020'):
    """
    Assemble a pypdfcodebook datastructure for exactly df's columns.

    Returns (datastructure, undocumented). Documents from, in order of
    precedence: this module's supplements, the HUA structure, the PREC
    structure. Prints any column it cannot document - fix the dictionary
    rather than shipping a codebook with silent gaps.
    """

    supplements = supplement_entries(basevintage)
    base = {}
    base.update(prec_v300_DataStructure)
    base.update(incore_v1_HUA_DataStructure)
    base.update(supplements)

    # House convention: primary_key names the table's key column.
    table_key = 'precid' if 'precid' in df.columns else 'huid'
    datastructure = {}
    undocumented = []
    for column in df.columns:
        if column in base:
            entry = dict(base[column])
            # The classic structures store pyType as a Python class;
            # pypdfcodebook prints it, so use the readable name.
            if isinstance(entry.get('pyType'), type):
                entry['pyType'] = entry['pyType'].__name__
            # pypdfcodebook's summary page hard-indexes these keys on every
            # entry, so guarantee them.
            entry.setdefault('primary_key', table_key)
            entry.setdefault('length', '')
            entry.setdefault('categorical',
                             'Categorical' if 'categories_dict' in entry else '')
            datastructure[column] = entry
        else:
            undocumented.append(column)

    if undocumented:
        print('UNDOCUMENTED COLUMNS (fix ncoda_09a before shipping the '
              'codebook):', undocumented)
    return datastructure, undocumented
