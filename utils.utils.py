import os
import json
import tempfile
import time
import pandas as pd
import google.auth
from google.cloud import firestore
from typing import List, Any

from core.firestore import Firestore

FS_COLLECTION_USERS = 'app_users'


def load_csv_to_dataframe(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath).dropna(how='all')
    return df


def load_excel_to_dataframe(filepath: str, sheet_name: Any = 0, header: Any = 0) -> pd.DataFrame:
    with (pd.ExcelFile(filepath) as xls):
        if (isinstance(sheet_name, str) and sheet_name in xls.sheet_names) or \
                (isinstance(sheet_name, int) and sheet_name < len(xls.sheet_names)):
            df = pd.read_excel(filepath, sheet_name=sheet_name, header=header).dropna(how='all')
            return df
    return pd.DataFrame()


def get_default_credentials() -> tuple:
    return google.auth.default()


def get_filename_by_date_pattern(pattern: str, prefix: str, postfix: str) -> str:
    return prefix + time.strftime(pattern) + postfix


def get_local_path() -> str:
    root = os.path.dirname(os.path.abspath(__file__))
    file_path = tempfile.gettempdir()
    return os.path.join(root, file_path)


def save_file_locally(df: pd.DataFrame, prefix: str = None) -> str:
    filename = get_filename_by_date_pattern('%Y%m%d', prefix, '.csv')
    filename_f = os.path.join(get_local_path(), filename)
    df.to_csv(filename_f, index=False, encoding='utf-8')
    return filename_f


def save_to_excel(dfs: List[pd.DataFrame], sheet_names: List[str], filename: str) -> None:
    if len(dfs):
        with pd.ExcelWriter(filename) as writer:
            for df, s in zip(dfs, sheet_names):
                df.to_excel(writer, sheet_name=s, index=False)


def get_config(project_id: str, collection_id: str, config_id: str) -> dict:
    # noinspection PyTypeChecker
    db = firestore.Client(project=project_id)
    doc_ref = db.collection(collection_id).document(config_id)

    doc = doc_ref.get()
    if doc.exists:
        return doc.to_dict()
    else:
        raise KeyError(f'key {config_id} not found in collection {collection_id}')


def add_firestore_routing(results: List[dict], collection_id: str, doc_id_field: str) -> List[dict]:
    results_augmented = []
    for r in results:
        rc = r.copy()
        rc.update({
            'fs_location': {
                'collection_id': collection_id,
                'doc_id': r[doc_id_field]
            }
        })
        results_augmented.append(rc)
    return results_augmented


# A set of functions to analyze App questionnaire answers
def split_hex(answer: str) -> tuple:
    if not answer.isnumeric():
        print(f'answer {answer} is not numeric')
        return None, None

    hex_number = int(answer)
    upper_16_bits = (hex_number >> 16) & 0xFFFF  # Shift right by 16 bits and apply a bitmask
    lower_16_bits = hex_number & 0xFFFF  # Apply a bitmask to get the lower 16 bits

    return lower_16_bits, upper_16_bits


def decrypt_baseline2h_answer(row: pd.Series) -> str:
    if not row['answer'].isdigit() or row['answer'] == '0' or row['question_id'] not in [12, 13]:
        return row['answer']

    answers = ''
    # answer_right = int(hex(int(row['answer']))[-4:], 16) if 'x' in hex(int(row['answer']))[-4:] \
    #     else int('0x' + hex(int(row['answer']))[-4:], 16)
    answer_right, _ = split_hex(row['answer'])
    for n in [2 ** i for i in range(0, 5)]:
        if n & answer_right:
            answers += str(n) + ', '

    return answers[:-2] if len(answers) else answers


def decrypt_daily_answer(row: pd.Series) -> str:
    if not row['answer'].isdigit() or row['answer'] == '0' or row['question_id'] not in [57, 58, 60, 62]:
        return row['answer']

    answers = ''
    # answer_right = int(hex(int(row['answer']))[-4:], 16) if 'x' in hex(int(row['answer']))[-4:] \
    #     else int('0x' + hex(int(row['answer']))[-4:], 16)
    answer_right, answer_left = split_hex(row['answer'])
    if row['question_id'] in [57, 58, 62]:
        for n in [2 ** i for i in range(0, 13)]:
            if n & answer_right:
                answers += str(n) + ', '
    else:
        # answer_left = int(hex(int(row['answer']))[:-4], 16) if int(row['answer']) >= 65536 else 0
        for n in [2 ** i for i in range(0, 14)]:
            if n & answer_right:
                answers += str(n) + 'h, ' if (n & answer_right) == (n & answer_left) else str(n) + ', '
    return answers[:-2] if len(answers) else answers


def lookup_intercom_id(fs: Firestore, message: dict) -> Any:
    docs = fs.read_docs(FS_COLLECTION_USERS, [message['userId']])
    if docs and 'intercom' in docs[0].keys():
        return docs[0]['intercom']['id']
    return None


class RequestMock:
    def __init__(self, configuration: Any) -> None:
        self._conf = configuration

    @property
    def headers(self):
        return self._conf.get('headers', {})

    @property
    def args(self):
        return self._conf

    def get_json(self) -> dict:
        return self._conf

    def get_text(self) -> str:
        return self._conf

    @property
    def data(self) -> bytes:
        return json.dumps(self._conf).encode('utf-8')
