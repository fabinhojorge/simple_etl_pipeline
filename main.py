from google.cloud import storage
import pandas as pd
import numpy as np 


BUCKET_NAME = '[BUCKET_NAME]/'


def convert_to_intbool(val_str):
    """Convert a String value in the Domain No/Yes into 0/1 respectively"""
    return 1 if val_str == 'Yes' else 0


def run_etl(_):
    """Entry point for the CLoud Function"""

    _ = storage.Client()  # Storage Client
    
    # Extract
    bom_name = "{0}{1}{2}".format("gs://", BUCKET_NAME, 'raw/bill_of_materials.csv')
    cb_name = "{0}{1}{2}".format("gs://", BUCKET_NAME, 'raw/comp_boss.csv')
    pq_name = "{0}{1}{2}".format("gs://", BUCKET_NAME, 'raw/price_quote.csv')
    
    bom = pd.read_csv(bom_name)
    cb = pd.read_csv(cb_name)
    pq = pd.read_csv(pq_name)

    # Transform
    cb["connection_type_id"] = cb["connection_type_id"].replace(to_replace="9999", value=np.nan)
    cb["height_over_tube"] = cb["height_over_tube"].replace(to_replace="9999", value=np.nan)
    cb['groove'] = cb.apply(lambda row: convert_to_intbool(row['groove']), axis=1)
    cb['unique_feature'] = cb.apply(lambda row: convert_to_intbool(row['unique_feature']), axis=1)
    cb['orientation'] = cb.apply(lambda row: convert_to_intbool(row['orientation']), axis=1)

    bom = pd.wide_to_long(
        df=bom,
        stubnames=["component_id", "quantity"],
        i="tube_assembly_id",
        j="id",
        sep="_"
    ).reset_index().sort_values(["tube_assembly_id", "id"], ignore_index=True)\
        .drop("id", axis=1).dropna(subset=["component_id"])

    pq['bracket_pricing'] = pq.apply(lambda row: convert_to_intbool(row['bracket_pricing']), axis=1)

    # Load
    bom_output_name = "{0}{1}{2}".format("gs://", BUCKET_NAME, 'refined/tube_assembly.csv')
    cb_output_name = "{0}{1}{2}".format("gs://", BUCKET_NAME, 'refined/component.csv')
    pq_output_name = "{0}{1}{2}".format("gs://", BUCKET_NAME, 'refined/price_quote.csv')

    bom.to_csv(bom_output_name, index=False)
    cb.to_csv(cb_output_name, index=False)
    pq.to_csv(pq_output_name, index=False)

    return f'ETL Ok. Check Data Studio.'
