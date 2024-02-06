import json
import logging
import os
import yaml
from bs4 import BeautifulSoup
from markdown import markdown
from .superset_api import Superset
import os

logging.basicConfig(level=logging.INFO)

def get_dataset_id_by_schema_table(datasets, schema, table):
    for dataset in datasets:
        if datasets[dataset]['schema'] == schema and datasets[dataset]['table_name'] == table:
            return datasets[dataset]['dataset_id']
    return None

def make_table_name(table, tags):
    return str(sorted(tags)).replace("'","") + " " + table


def main(datasets_dir, superset_db_id, superset_refresh_columns, superset):
    datasets_superset = superset.get_datasets(superset_db_id)

    input_datasets={}
    for file in os.listdir(datasets_dir):
        if file.endswith(".yml"):            
            noext_filename = os.path.splitext(file)[0]
            yml_filename = file
            sql_filename = f"{noext_filename}.sql"

            if not os.path.exists(os.path.join(datasets_dir,sql_filename)):
                print(f"The SQL file '{sql_filename}' does not exist.")

            with open(os.path.join(datasets_dir, yml_filename), 'r') as y, open(os.path.join(datasets_dir, sql_filename), 'r') as s:
                yaml_data = yaml.safe_load(y)
                input_datasets[noext_filename] = yaml_data
                if 'columns' not in input_datasets[noext_filename]:
                    input_datasets[noext_filename]['columns'] = []
                input_datasets[noext_filename]["sql"] = s.read()                

    for i in input_datasets:
        # refresh columns
        superset.refresh_dataset(i)
        
        # get columns from dataset definition
        columns_from_yml = { x['name'].upper() : x for x in input_datasets[i]['columns'] }

        # get descriptions from propagated columns from parent datasets in superset
        columns_from_propagation = {}

        for j in reversed(input_datasets[i].get('propagate_columns_from', [])):
            ds_id = get_dataset_id_by_schema_table(datasets_superset, j['schema'], j['table'])
            if ds_id is None:
                logging.error("The dataset %s.%s does not exist in Superset. Please check your propagate_columns_from section in %s.yml.", j['schema'], j['table'], i)
                continue
            cols = { x['column_name'].upper() : x for x in superset.get_columns(ds_id)['columns'] if x.get('description') is not None}
            columns_from_propagation |= cols

        # get columns from superset's dataset
        keys_allowed_to_update=['advanced_data_type', 'column_name', 'description', 'expression', 'extra', 'filterable', 'groupby', 'id', 'is_active', 'is_dttm', 'python_date_format', 'type', 'uuid', 'verbose_name']
        columns_from_ds = [{key: value for key, value in item.items() if key in keys_allowed_to_update} for item in superset.get_columns(i)['columns']]


        for c in columns_from_ds:
            if c['column_name'] in columns_from_propagation:
                c['description'] = columns_from_propagation[c['column_name']]['description']
                
                if c['type'] is None:
                    c['type'] = columns_from_propagation[c['column_name']]['type']
                
                if c['verbose_name'] is None:
                    c['verbose_name'] = columns_from_propagation[c['column_name']]['verbose_name']
                
            if c['column_name'] in columns_from_yml:
                c |= columns_from_yml[c['column_name']]

            # if we don't have a type, we don't want to send it to superset
            if "type" not in c or c['type'] is None:
                del c['type']

            # name is not a valid column property, table_name is constructed below
            if "name" in c:
                c['column_name'] = c.pop('name')

            if "is_filterable" in c:
                c['filterable'] = c.pop('is_filterable')

            if "is_groupable" in c:
                c['groupby'] = c.pop('is_groupable')
       

        ds={}
        ds['table_name']=make_table_name(input_datasets[i]['name'], input_datasets[i]['tags'])
        ds['description']=input_datasets[i]['description']
        ds['sql']=input_datasets[i]['sql']
        #ds['filter_select_enabled']=filter_value_extraction.enable
        #ds['fetch_values_predicate']=filter_value_extraction.where
        ds['cache_timeout']=input_datasets[i]['results_cache_timeout_seconds']
        ds['is_managed_externally']=True
        ds['extra'] = json.dumps({
                "certification" : {
                    "certified_by": "Data Platform Team",
                    "details": "dbt-managed, embeddable virtual dataset"
                    }
                })        
        ds['database_id']=superset_db_id
        ds['metrics']=[
            {
                "metric_name": x['name'],
                "verbose_name": x.get('verbose_name',x['name']),
                "expression": x['expression'],
                "description": x.get('description',''),
                "d3format": x['d3_format'],
                "warning_text": x.get('warning','')
            } for x in input_datasets[i].get('metrics',[]) 
        ]
        ds['owners']=[1]
        ds['columns']=columns_from_ds


        # clear existing metrics (failing to do this results in HTTP 422)
        superset.update_virtual_dataset(i, {"metrics":[]})

        # update dataset
        superset.update_virtual_dataset(i, ds)       


