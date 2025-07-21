import numpy as np
import pandas as pd
import re
from os import path
from datetime import datetime
from core.bigquery import Bigquery
from core.storage import Storage
from utils.utils import RequestMock, get_config, get_default_credentials, load_csv_to_dataframe, load_excel_to_dataframe

credentials, project = get_default_credentials()

FS_COLLECTION_CONFIGS = 'configs_services'
FS_DOCUMENT_CONFIG_ID = 'srv-data-listener-procare'
SQL_SCRIPT_LOCATION = 'sql'

schema_rx_procare_append = [
    "De_identified_Patient_ID", "Rx_Number", "Received_Date", "Dispense_Date", "Serial__", "Total_Fills",
    "Fills_Dispensed", "Fill_Remaining", "Provider_Last_Name", "Provider_First_Name", "Provider_Address",
    "Provider_City", "Provider_State__", "Provider_Zip_Code", "Provider_NPI", "Region", "Script_Status", "Patient_OOP",
    "Payor_Name", "Plan_Name", "Copay", "Source", "Fill_Type_Recieved", "Fill_Type_Shipped", "Date_Written",
    "CLOSED_STATUS", "Insurance_Type", "PA_STATUS", "Order_PA_Status", "REMINDERSTATUS_PAT", "Plan_Name_Claim", "AGE",
    "NDC", "USAGE", "modified_serial_id", "_snapshot_date"
]
schema_bi_summary = [
    'PATID', 'RX_NUM', 'DATE_ENTERED', 'WE_DATE_ENTERED_MED_BI', 'STATUS_ID', 'SUBCATEGORY_MED_BI',
    'MEDBISTATUS', 'INS_PLN', 'MEDICAL_PLN_NAME', 'RX_REJ_CODE', 'RX_BIN', 'RX_PCN', 'RX_PBM', 'DR_NPI',
    'DR_NAME', 'DR_ADD', 'DR_ST', 'DR_ZIP', 'DATE_PA_FAXED_MED_BI', 'PRECERTIFICATION', 'DATE_CLM_SUBMT_MED_BI',
    'MED_BILLING_STATUS', 'SCA__APPROVED_DATE', 'SCA_STATUS_MED_BI', 'MIDAS_CODE_BI', 'SUPPORT_DOCS_MD_MED_BI',
    'ICD_10_MED_BI', 'TRIED_FAILED_BI', 'SERIAL_NUMBER', 'ACUTE_PREVENTION', 'DATEWRITTEN', 'DATE_RESP_LTR_RECD_BI',
    'DATE_CLINCL_DOC_REQ', 'APPL_STATUS_MED_BI', 'DATE_APPL_FAXED_HCP_MED_BI', 'DATE_APPL_FAXED_INS_MED_BI',
    'DATE_APPL_DENIED_MED_BI', 'CLAIM_REJECT', 'MED_CLAIM_PAYMENT', 'MED_APPLIED_DEDUCTIBLE', 'MED_PAT_COPAY_CO_INS'
]


def clean_serial(sn: str) -> str:
    if pd.isna(sn):
        return sn
    sn = str(sn).strip()
    match = re.search(r'(NM|NI)[\w-]+', sn)
    return match.group(0) if match else sn


def process_dataframe_rx_procare(df: pd.DataFrame) -> pd.DataFrame:
    def convert_to_int(value, default):
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    if df['Provider NPI'].dtype == 'str':
        df['Provider NPI'] = df['Provider NPI'].str.extract('(\d+)', expand=False).fillna(0).astype(int)
    elif df['Provider NPI'].dtype == 'float64':
        df['Provider NPI'] = df['Provider NPI'].fillna(0).astype(int)

    df['Total Fills'] = df['Total Fills'].apply(lambda v: convert_to_int(v, 0)).astype(int)
    df['Fills Dispensed'] = df['Fills Dispensed'].apply(lambda v: convert_to_int(v, 0)).astype(int)
    df['Fill Remaining'] = df['Fill Remaining'].fillna(0).astype(int)
    df['Rx Number'] = df['Rx Number'].fillna(0).astype(int)
    df['Provider Zip Code'] = pd.to_numeric(df['Provider Zip Code'], errors='coerce').fillna(0).astype(int)
    df['Patient OOP'] = df['Patient OOP'].astype(str).str.replace('[$(),]', '', regex=True).astype(float).fillna(0)
    df['Copay'] = df['Copay'].str.replace('[$(),]', '', regex=True).astype(float).fillna(0)
    df['Region'] = df['Region'].apply(lambda v: 'N/A' if np.isnan(v) else v).astype(str)
    df['Received Date'] = pd.to_datetime(df['Received Date']).dt.date
    df['Dispense Date'] = pd.to_datetime(df['Dispense Date']).dt.date
    df['Date Written'] = pd.to_datetime(df['Date Written']).dt.date
    df['Serial #'] = df['Serial #'].apply(clean_serial)
    df['modified_serial_id'] = df['Serial #']
    # df.rename(columns={'De-identified Patient ID': 'De_identified_Patient_ID', 'Serial #': 'Serial__'}, inplace=True)
    grouped = df.groupby('De-identified Patient ID')
    for pid, group in grouped:
        serials = group['Serial #'].apply(lambda x: '' if pd.isna(x) else str(x).strip())
        # serials = group['Serial #'].astype(str).fillna('')

        refill_mask = (
                group['Dispense Date'].notna() &
                (group['NDC'] == 90017578200) &
                # (group['CLOSED_STATUS'] == 'SHIPPED') &
                (
                        serials.str.strip().eq('') |
                        serials.str.contains('DL2432570', na=False)
                )
        )
        # refill_mask = (
        #         group['Dispense Date'].notna() &
        #         (group['NDC'] == 90017578200) &
        #         (group['Serial #'].isna() | (group['Serial #'].astype(str).str.strip() == ''))
        # )
        refill_indexes = group[refill_mask].index

        original_mask = group['Serial #'].fillna('').astype(str).str.startswith('NI')
        original_serials = group.loc[original_mask, 'Serial #'].unique()

        if len(original_serials) == 1:
            original_serial = original_serials[0]
            for i, idx in enumerate(refill_indexes, start=1):
                df.at[idx, 'modified_serial_id'] = f"{original_serial}refill{i}"

    df['_snapshot_date'] = datetime.today().strftime("%Y-%m-%d")
    df.columns = schema_rx_procare_append
    return df


def process_dataframe_bi_summary(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.replace('_ ', '_').str.replace(' ', '_').str.replace('/', '_').str.replace('-', '_').str.replace(' _', '_')
    column_rename = {'CLAIM_PAYMENT': 'MED_CLAIM_PAYMENT', 'APPLIED_DEDUCTIBLE': 'MED_APPLIED_DEDUCTIBLE', 'PAT_COPAY_COINS': 'MED_PAT_COPAY_CO_INS'}
    df.rename(columns=column_rename, inplace=True)
    df = df[schema_bi_summary].copy()
    df['SUPPORT_DOCS_MD_MED_BI'] = df['SUPPORT_DOCS_MD_MED_BI'].astype(str)
    df['TRIED_FAILED_BI'] = df['TRIED_FAILED_BI'].astype(str)
    df['DATE_APPL_FAXED_HCP_MED_BI'] = df['DATE_APPL_FAXED_HCP_MED_BI'].astype(str)
    df['DATE_APPL_FAXED_INS_MED_BI'] = df['DATE_APPL_FAXED_INS_MED_BI'].astype(str)
    df['DATE_APPL_DENIED_MED_BI'] = df['DATE_APPL_DENIED_MED_BI'].astype(str)
    df['MED_CLAIM_PAYMENT'] = df['MED_CLAIM_PAYMENT'].fillna('no data').astype(str)
    df['RX_NUM'] = df.RX_NUM.apply(pd.to_numeric, errors='coerce').fillna(0).astype(int)
    df['DATE_ENTERED'] = pd.to_datetime(df['DATE_ENTERED'])
    df['DATEWRITTEN'] = df['DATEWRITTEN'].apply(pd.to_datetime, errors='coerce').fillna(df['DATE_ENTERED'])
    df['WE_DATE_ENTERED_MED_BI'] = df['WE_DATE_ENTERED_MED_BI'].apply(pd.to_datetime, errors='coerce').fillna(df['DATE_ENTERED'])
    df['MIDAS_CODE_BI'] = df['MIDAS_CODE_BI'].fillna(0).astype(int)
    df['PATID'] = df['PATID'].fillna(0).astype(int)
    df['DR_ZIP'] = pd.to_numeric(df['DR_ZIP'], errors='coerce').astype('Int64')
    df['_snapshot_date'] = datetime.today().strftime("%Y-%m-%d")
    return df


def is_local_run() -> bool:
    return False  # todo change to False in production


def run(event, context):
    filename, bucket = event['name'], event['bucket']
    print(f'{filename} from bucket {bucket} has triggered a function run..')
    config = get_config(project, FS_COLLECTION_CONFIGS, FS_DOCUMENT_CONFIG_ID)

    if 'PROCARE_THERANICA_ITD_DATAFEED' in filename.upper():
        bq = Bigquery()

        table_path = '.'.join([config['rx_procare']['bigquery_dataset'], config['rx_procare']['bigquery_tableid']])
        local_path = Storage().download_blob(bucket, filename)
        df = load_csv_to_dataframe(local_path)
        df = process_dataframe_rx_procare(df)
        df.to_csv(local_path, index=False)
        bq.load_from_local(local_path, bq.FileType.CSV, bq.WriteMode.APPEND, table_path,
                           {Bigquery.LoadJobConfig.SKIP_LEADING_ROWS: 1})

        # run post upload etl
        bq.run_dml_script_from_path(path.join(SQL_SCRIPT_LOCATION, 'procare_etl.sql'))
        bq.run_dml_script_from_path(path.join(SQL_SCRIPT_LOCATION, 'rx_procare.sql'))
        bq.run_append_script('select * from staging.rx_procare_tmp;', '.'.join([project, 'dwh', 'rx_pharmacies']))
        bq.run_dml_script_from_path(path.join(SQL_SCRIPT_LOCATION, 'procare_mock_remove.sql'))
    elif 'BI SUMMARY' in filename.upper():
        bq = Bigquery()

        table_path = '.'.join([config['bi_summary']['bigquery_dataset'], config['bi_summary']['bigquery_tableid']])
        local_path = Storage().download_blob(bucket, filename)
        df = load_excel_to_dataframe(local_path)
        df = process_dataframe_bi_summary(df)
        df.to_csv(local_path, index=False)
        bq.load_from_local(local_path, bq.FileType.CSV, bq.WriteMode.APPEND, table_path,{Bigquery.LoadJobConfig.SKIP_LEADING_ROWS: 1})

        # # run post upload etl
        bq.run_dml_script_from_path(path.join(SQL_SCRIPT_LOCATION, 'bi_summary.sql'))
        bq.run_append_script('select * from staging.rx_procare_bisummary_tmp;',
                             '.'.join([project, 'dwh', 'rx_pharmacies']))
    else:
        print(f'unrecognized file uploaded - {filename}')

    return 'OK'


# FOR LOCAL TESTING ############################################################################################
if __name__ == '__main__':
    # pandas settings
    pd.options.display.max_columns = None
    pd.options.display.max_rows = None
    pd.options.display.width = 200

    source_filename = 'ProCare_THERANICA_ITD_DATAFEED_2025-05-05.csv'
    source_bucket = 'neriviodata-prod-procare-daily'
    request_mock = RequestMock({'name': source_filename, 'bucket': source_bucket})
    run(request_mock.get_json(), None)
