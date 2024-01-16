def get_dataset_id_by_schema_table(datasets, schema, table):
    for dataset in datasets:
        if datasets[dataset]['schema'] == schema and datasets[dataset]['table_name'] == table:
            return datasets[dataset]['dataset_id']
    return None

def make_table_name(table, tags):
    return str(sorted(tags)).replace("'","") + " " + table