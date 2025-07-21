from collections import namedtuple
from typing import List, Any
from google.cloud import firestore

FS_BATCH_SIZE = 500
UpdateScope = namedtuple('UpdateScope', ['key', 'field', 'values'])


class Firestore:
    def __init__(self, project_id: str, verbose: bool = False) -> None:
        # noinspection PyTypeChecker
        self._db = firestore.Client(project_id)
        self._verbose = verbose

    def update(self, collection_id: str, docs: List[dict]) -> None:
        print(f'about to update {len(docs)} records in firestore.. ({collection_id})')
        batch = self._db.batch()
        rec_count = 0

        for i, doc in enumerate(docs):
            doc_ref = self._db.collection(collection_id).document(doc['key'])
            batch.update(doc_ref, doc['payload']) if doc_ref.get().exists else batch.set(doc_ref, doc['payload'])
            print(f'{doc["key"]} is about to be committed to firestore..')
            if i and i % FS_BATCH_SIZE == 0:
                batch.commit()
                print(f'{FS_BATCH_SIZE} records committed to firestore..')
            rec_count = 1

        if rec_count % FS_BATCH_SIZE != 0:
            batch.commit()
            print(f'{rec_count} record(s) committed to firestore..')

    def update_array_add(self, collection_id: str, doc: dict) -> None:
        update_scope = UpdateScope(doc['key'], doc['payload']['field'], doc['payload']['values'])
        doc_ref = self._db.collection(collection_id).document(update_scope.key)
        if isinstance(update_scope.values, list):
            doc_ref.update({update_scope.field: firestore.ArrayUnion(list(set(update_scope.values)))})
        elif isinstance(update_scope.values, dict):
            doc_ref.update({update_scope.field: firestore.ArrayUnion([update_scope.values])})
        print(f'{update_scope.values} added to field {update_scope.field} ({update_scope.key})')

    def update_array_archive(self, collection_id: str, doc: dict) -> None:
        update_scope = UpdateScope(doc['key'], doc['payload']['field'], doc['payload']['values'])
        doc_ref = self._db.collection(collection_id).document(update_scope.key)
        doc_ref.update({update_scope.field + '_archive': firestore.ArrayUnion(list(set(update_scope.values)))})
        print(f'{update_scope.values} archived for field {update_scope.field} ({update_scope.key})')

    def update_array_unarchive(self, collection_id: str, doc: dict) -> None:
        update_scope = UpdateScope(doc['key'], doc['payload']['field'], doc['payload']['values'])
        doc_ref = self._db.collection(collection_id).document(update_scope.key)
        doc_ref.update({update_scope.field + '_archive': firestore.ArrayRemove(list(set(update_scope.values)))})
        print(f'{update_scope.values} un-archived for field {update_scope.field} ({update_scope.key})')

    def update_array_remove(self, collection_id: str, doc: dict) -> None:
        update_scope = UpdateScope(doc['key'], doc['payload']['field'], doc['payload']['values'])
        doc_ref = self._db.collection(collection_id).document(update_scope.key)
        doc_ref.update({update_scope.field: firestore.ArrayRemove(list(set(update_scope.values)))})
        print(f'{update_scope.values} removed from field {update_scope.field} ({update_scope.key})')

    def read_docs(self, collection_id: str, doc_ids: List[str]) -> List[dict]:
        docs = []
        if len(doc_ids):
            for doc_id in doc_ids:
                doc = self._db.collection(collection_id).document(doc_id).get()
                docs.append(doc.to_dict()) if doc.exists \
                    else print(f'key {doc_id} not found in collection {collection_id}')
            return docs
        return [doc.to_dict() for doc in self._db.collection(collection_id).stream()]     # all documents in collection

    def read_docs_by_field(self, collection_id: str, field_id: str) -> List[dict]:
        docs = self._db.collection(collection_id).order_by(field_id).get()
        return [doc.to_dict() for doc in docs] if len(docs) else []

    def read_docs_by_where(self, collection_id: str, field_path: str, op_string: str, value: Any) -> List[dict]:
        docs = self._db.collection(collection_id).where(field_path, op_string, value).get()
        return [doc.to_dict() for doc in docs] if len(docs) else []
