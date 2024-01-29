import json
import logging
import os
import re

from bs4 import BeautifulSoup
from markdown import markdown


logging.basicConfig(level=logging.INFO)

def get_tables_from_dbt(dbt_manifest, dbt_db_name):
    tables = {}
    for table_type in ['nodes', 'sources']:
        manifest_subset = dbt_manifest[table_type]

        for table_key_long in manifest_subset:
            table = manifest_subset[table_key_long]

            if dbt_db_name is None or table['database'] == dbt_db_name:
                name = table.get("alias", table['name'])
                schema = table['schema']            
                table_key_short = schema + '.' + name

                tables[table_key_short] = {'columns': table['columns'],
                                           'meta': table['meta'],
                                           'description': table.get('description', table.get('config',{}).get('description'))}

    assert tables, "Manifest is empty!"

    return tables

def get_auto_register_tables(dbt_tables):    
    return [k for k, v in dbt_tables.items() if v.get('meta').get('bi_integration', {}).get('auto_register', False)]

def filter_by_kind(sst_datasets, kind):
    return {k:v for k, v in sst_datasets.items() if v.get('kind') == kind}

def convert_markdown_to_plain_text(md_string):
    """Converts a markdown string to plaintext.

    The following solution is used:
    https://gist.github.com/lorey/eb15a7f3338f959a78cc3661fbc255fe
    """

    # md -> html -> text since BeautifulSoup can extract text cleanly
    html = markdown(md_string)

    # remove code snippets
    html = re.sub(r'<pre>(.*?)</pre>', ' ', html)
    html = re.sub(r'<code>(.*?)</code >', ' ', html)

    # extract text
    soup = BeautifulSoup(html, 'html.parser')
    text = ''.join(soup.findAll(text=True))

    # make one line
    single_line = re.sub(r'\s+', ' ', text)

    # make fixes
    single_line = re.sub('→', '->', single_line)
    single_line = re.sub('<null>', '"null"', single_line)

    return single_line

def merge_columns_info(dataset, dbt_tables, debug_dir):
    logging.info("Merging columns info from Superset and manifest.json file.")

    key = dataset['name']
    id = dataset['id']

    meta_sst = dataset['meta']
    
    # add the whole dbt meta object of the table/model to the dict for debugging purpose
    meta_dbt = dbt_tables.get(key, {}).get('meta', {})
    dataset['meta_dbt'] = meta_dbt

    # Add meta information of the dataset.
    meta_new = {}
    # FIXME: The name of this and related functions is not correctly scoped any longer,
    # as we don't only process columns but also the metadata...

    # Prepopulate the dataset's meta data in case that the dataset is NOT set
    # to be managed externally (i.e., by dbt).
    # The dbt meta data field `prohibit_manual_editing` decides whether the dataset
    # is externally managed, NOT Superset's metadata!

    meta_new['is_managed_externally'] = meta_dbt.get('bi_integration', {}).get('prohibit_manual_editing', False)

    if not meta_new['is_managed_externally']:
        for field in ['cache_timeout', 'description', 'fetch_values_predicate', 'filter_select_enabled', 'main_dttm_col']:
            if meta_sst[field] is not None:
                meta_new[field] = meta_sst[field]

    dbt_description = dbt_tables.get(key, {}).get('description')
    if dbt_description is not None:
        meta_new['description'] = convert_markdown_to_plain_text(dbt_description)

    # Not sure if we need to suppress None values here?!
    meta_new['cache_timeout'] = meta_dbt.get('bi_integration', {}).get('results_cache_timeout_seconds')
    meta_new['fetch_values_predicate'] = meta_dbt.get('bi_integration', {}).get('filter_value_extraction', {}).get('where')
    meta_new['filter_select_enabled'] = meta_dbt.get('bi_integration', {}).get('filter_value_extraction', {}).get('enable')
    meta_new['main_dttm_col'] = meta_dbt.get('bi_integration', {}).get('main_timestamp_column')
    meta_new['owners'] = meta_dbt.get('owners', [])

    # Populate the dataset's `extra` field:

    # Handle dataset certification:
    # We only inlcude a certification if the certified_by is not empty
    # The certification details then contains a concatenation of the certification.details field
    # and the model_maturity field, if exists.
    certification = meta_dbt.get('certification') or dict()
    certified_by = certification.get('certified_by')
    extra_dict = dict()

    if certified_by is not None:
        if meta_dbt.get('model_maturity') is not None:
            model_maturity = 'maturity: ' + meta_dbt.get('model_maturity')
        else:
            model_maturity = None
        # Write to list and remove empty elements, so we end up with a clean looking concatenation:
        certification_details_list = [certification.get('details'), model_maturity]
        certification_details_list = [i for i in certification_details_list if i is not None]
        certification_details = '; '.join(certification_details_list)
        extra_dict['certification'] = {"certified_by": certified_by, "details": certification_details}

    # Add warning_markdown field if exists:
    warning_markdown = meta_dbt.get('bi_integration', {}).get('warning_markdown')
    if warning_markdown is not None:
        extra_dict['warning_markdown'] = warning_markdown

    # The `extra` field requires escaped JSON as value:
    extra_json = json.dumps(extra_dict)
    meta_new['extra'] = extra_json


    dataset['meta_new'] = meta_new

    # Columns:
    sst_columns = dataset['columns']
    dbt_columns = dbt_tables.get(key, {}).get('columns', {})

    if debug_dir is not None:
        # Superset columns:
        superset_columns_file_path = os.path.join(debug_dir, f'superset_columns__dataset_{id}.json')
        with open(superset_columns_file_path, 'w') as fp:
            json.dump(sst_columns, fp, sort_keys=True, indent=4)

        # dbt columns:
        dbt_columns_file_path = os.path.join(debug_dir, f'dbt_columns__dataset_{id}.json')
        with open(dbt_columns_file_path, 'w') as fp:
            json.dump(dbt_columns, fp, sort_keys=True, indent=4)


    columns_new = []
    for sst_column in sst_columns:

        # add the mandatory field
        column_name = sst_column['column_name'].lower()
        column_new = {'column_name': column_name.upper()}

        # Note: type_generic and created_on cannot be included, apparently.
        preserve_fields_list = [
            'column_name', 
            'description', 
            'expression', 
            'filterable', 
            'groupby', 
            'verbose_name',
            'type',
            'advanced_data_type',
            'extra',
            'is_active',
            'is_dttm',
            'python_date_format'
        ]

        
        for field in preserve_fields_list:
            if sst_column[field] is not None:
                column_new[field] = sst_column[field]

        # In any case, set `is_dttm`` based on the data type determined by Superset;
        # currently there we have no dbt `meta` field assigned for this, as it should not be needed this way.
        if sst_column.get('type') in ['DATE', 'TIMESTAMP'] or sst_column.get('is_dttm') == True:
            column_new['is_dttm'] = True
            # DEBUG
            # logging.info("Column %s of datased %d is temporal", column_name, id)


        # We always overwrite the following fields from dbt's settings:

        if column_name in dbt_columns \
                and 'description' in dbt_columns[column_name]:
            description = dbt_columns[column_name]['description']
            description = convert_markdown_to_plain_text(description)
        else:
            description = sst_column['description']
        column_new['description'] = description

        # Meta fields:
        # The column meta fields are called differently in Superset and thus need to be renamed.
        # For this reason this code is not DRY for now...

        # add verbose_name which is in the `meta` dict in dbt
        if column_name in dbt_columns \
                and 'verbose_name' in dbt_columns[column_name]['meta']:
            verbose_name = dbt_columns[column_name]['meta']['verbose_name']
            column_new['verbose_name'] = verbose_name
        else:
            # Fall back to Title Cased column_name
            column_new['verbose_name'] = column_name.replace('_', ' ').title()

        # Append unit to verbose_name, if present:
        if column_name in dbt_columns:
            unit = dbt_columns[column_name]['meta'].get('unit', None)
            if unit is not None:
                column_new['verbose_name'] = column_new['verbose_name'] + f' [{unit}]'

        # add is_filterable which is in the `meta` dict in the 'bi_integration' section
        if column_name in dbt_columns \
                and 'is_filterable' in dbt_columns[column_name]['meta'].get('bi_integration', {}):
            is_filterable = dbt_columns[column_name]['meta']['bi_integration']['is_filterable']
            column_new['filterable'] = is_filterable
            # DEBUG
            # logging.info("Column %s is filterable?: %s", column_name, is_filterable)

        # add is_groupable which is in the `meta` dict in the 'bi_integration' section
        if column_name in dbt_columns \
                and 'is_groupable' in dbt_columns[column_name]['meta'].get('bi_integration', {}):
            is_groupable = dbt_columns[column_name]['meta']['bi_integration']['is_groupable']
            column_new['groupby'] = is_groupable
            # DEBUG
            # logging.info("Column %s is groupable?: %s", column_name, is_groupable)

        columns_new.append(column_new)

    dataset['columns_new'] = columns_new

    return dataset

def main(dbt_project_dir, dbt_db_name, superset_db_id, superset_debug_dir, superset_refresh_columns, superset):

    logging.info("Getting datasets from Superset.")
    sst_datasets = superset.get_datasets(superset_db_id)

    sst_physical_datasets = filter_by_kind(sst_datasets, 'physical')
    logging.info("There are %d physical datasets in Superset.", len(sst_physical_datasets))

    sst_virtual_datasets = filter_by_kind(sst_datasets, 'virtual')
    logging.info("There are %d virtual datasets in Superset.", len(sst_virtual_datasets))

    logging.info("Reading manifest.json.")
    with open(f'{dbt_project_dir}/target/manifest.json') as f:
        dbt_manifest = json.load(f)

    dbt_tables = get_tables_from_dbt(dbt_manifest, dbt_db_name)
    logging.info("There are %d datasets in DBT.", len(dbt_tables))

    # Auto-registration of dbt models in Superset:
    # Which tables are set to be auto-registered in dbt and are not yet present in Superset?:
    auto_register_tables = get_auto_register_tables(dbt_tables)

    # Which of these are not yet present in Superset
    tables_to_register = [table for table in auto_register_tables if table not in sst_physical_datasets]

    # check if the names we want to use are not occupied by virtual datasets
    # in case they are, we need to rename them
    datasets_to_rename = {k: v for k, v in sst_virtual_datasets.items() if k in tables_to_register}
    for table in datasets_to_rename:
        superset.rename_dataset(datasets_to_rename[table]['dataset_id'],
                               datasets_to_rename[table]["schema"] + ".[renamed] " + datasets_to_rename[table]["table_name"] )

    # Register them
    for table in tables_to_register:
        try:
            superset.create_physical_dataset(superset_db_id, table)
        except Exception as e:
            logging.error("The database table %s could not be registered. %s",
                          table, e.response.json()['message'])

    # Re-fetch Superset datasets, as we have just registered new ones
    sst_datasets = superset.get_datasets(superset_db_id)
    sst_physical_datasets = filter_by_kind(sst_datasets, 'physical')
    logging.info("There are %d physical datasets in Superset.", len(sst_physical_datasets))

    for sst_dataset in sst_physical_datasets:
        sst_dataset_id = sst_physical_datasets[sst_dataset]['dataset_id']

        logging.info("Processing dataset ID: %d, name: %s.", sst_dataset_id, sst_dataset)

        # Only process datasets which exist in dbt:
        if sst_dataset in dbt_tables:
            try:
                if superset_refresh_columns:
                    superset.refresh_dataset(sst_dataset_id)
                sst_dataset_w_cols = superset.get_columns(sst_dataset_id)
                sst_dataset_w_cols_new = merge_columns_info(sst_dataset_w_cols, dbt_tables, superset_debug_dir)
                superset.put_columns(sst_dataset_w_cols_new, superset_debug_dir)
            except Exception as e:
                logging.error("The dataset named %s with ID=%d wasn't updated. Check the error below.",
                            sst_dataset, sst_dataset_id, exc_info=e)

    logging.info("All done!")
