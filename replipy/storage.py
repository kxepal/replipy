# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import hashlib
import pickle
import time
import uuid
from abc import ABCMeta, abstractmethod
from collections import defaultdict


class ABCDatabase(object, metaclass=ABCMeta):

    class Conflict(Exception):
        """Raises in case of conflict updates"""

    def __init__(self, name):
        self._name = name
        self._start_time = int(time.time() * 10**6)
        self._update_seq = 0

    @abstractmethod
    def __contains__(self, idx):
        """Verifies that document with specified idx exists"""

    @property
    def name(self):
        """Returns database symbolic name as string"""
        return self._name

    @property
    def start_time(self):
        """Returns database start time in microseconds"""
        return self._start_time

    @property
    def update_seq(self):
        """Returns current update sequence value"""
        return self._update_seq

    def info(self):
        """Returns database information object as dict"""
        return {
            'db_name': self.name,
            'instance_start_time': str(self.start_time),
            'update_seq': self.update_seq
        }

    @abstractmethod
    def load(self, idx):
        """Returns document by specified idx"""

    @abstractmethod
    def store(self, doc, rev=None):
        """Creates document or updates if rev specified"""

    @abstractmethod
    def remove(self, idx, rev):
        """Removes document by specified idx and rev"""

    @abstractmethod
    def revs_diff(self, idrevs):
        """Returns missed revisions for specified id - revs mapping"""

    @abstractmethod
    def bulk_docs(self, docs, new_edits=True):
        """Bulk update docs"""

    @abstractmethod
    def ensure_full_commit(self):
        """Ensures that all changes are actually stored on disk"""


class MemoryDatabase(ABCDatabase):

    def __init__(self, *args, **kwargs):
        super(MemoryDatabase, self).__init__(*args, **kwargs)
        self._docs = {}

    def __contains__(self, item):
        return item in self._docs

    def _new_rev(self, doc):
        oldrev = doc.get('_rev')
        if oldrev is None:
            seq, _ = 0, None
        else:
            seq, _ = oldrev.split('-', 1)
            seq = int(seq)
        sig = hashlib.md5(pickle.dumps(doc)).hexdigest()
        newrev = '%d-%s' % (seq + 1, sig)
        return newrev.lower()

    def load(self, idx):
        return self._docs[idx]

    def store(self, doc, rev=None, new_edits=True):
        if '_id' not in doc:
            doc['_id'] = str(uuid.uuid4()).lower()
        if rev is None:
            rev = doc.get('_rev')

        idx = doc['_id']

        if new_edits:
            if idx in self and self._docs[idx]['_rev'] != rev:
                raise self.Conflict('Document update conflict')
            elif idx not in self and rev is not None:
                raise self.Conflict('Document update conflict')
            doc['_rev'] = self._new_rev(doc)
        else:
            assert rev, 'Document revision missed'
            doc['_rev'] = rev

        idx, rev = doc['_id'], doc['_rev']

        self._docs[idx] = doc
        self._update_seq += 1

        return idx, rev

    def remove(self, idx, rev):
        if self._docs[idx]['_rev'] != rev:
            raise self.Conflict('Document update conflict')
        doc = {
            '_id': idx,
            '_rev': rev,
            '_deleted': True
        }
        return self.store(doc, rev)

    def revs_diff(self, idrevs):
        res = defaultdict(dict)
        for idx, revs in idrevs.items():
            missing = []
            if idx not in self:
                missing.extend(revs)
                res[idx]['missing'] = missing
                continue
            doc = self._docs[idx]
            for rev in revs:
                if doc['_rev'] != rev:
                    missing.append(rev)
            if missing:
                res[idx]['missing'] = missing
        return res

    def bulk_docs(self, docs, new_edits=True):
        res = []
        for doc in docs:
            idx, rev = doc['_id'], doc.get('_rev')
            try:
                idx, rev = self.store(doc, rev, new_edits)
                res.append({
                    'ok': True,
                    'id': idx,
                    'rev': rev
                })
            except Exception as err:
                res.append({'id': idx,
                            'error': type(err).__name__,
                            'reason': str(err)})
        return res

    def ensure_full_commit(self):
        return {
            'ok': True,
            'instance_start_time': self.info()['instance_start_time']
        }
