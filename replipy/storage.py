# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import base64
import hashlib
import pickle
import time
import uuid
from abc import ABCMeta, abstractmethod
from collections import defaultdict

_MetaDatabase = ABCMeta('_MetaDatabase', (object,), {})

class ABCDatabase(_MetaDatabase):

    class Conflict(Exception):
        """Raises in case of conflict updates"""

    class NotFound(Exception):
        """Raises in case attempt to query on missed document"""

    def __init__(self, name):
        self._name = name
        self._start_time = int(time.time() * 10**6)
        self._update_seq = 0

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
    def contains(self, idx, rev=None):
        """Verifies that document with specified idx exists"""

    @abstractmethod
    def check_for_conflicts(self, idx, rev):
        """Check that specified idx and rev provides no conflicts
        or raises Conflict exception otherwise"""

    @abstractmethod
    def load(self, idx, rev=None):
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

    @abstractmethod
    def changes(self, since=0, feed='normal', style='all_docs', filter=None):
        """Ensures that all changes are actually stored on disk"""

    @abstractmethod
    def add_attachment(self, doc, name, data, ctype='application/octet-stream'):
        """Adds attachment to specified document"""


class MemoryDatabase(ABCDatabase):

    def __init__(self, *args, **kwargs):
        super(MemoryDatabase, self).__init__(*args, **kwargs)
        self._docs = {}
        self._changes = {}

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

    def check_for_conflicts(self, idx, rev):
        if self.contains(idx):
            if rev is None:
                if idx.startswith('_local/'):
                    return
                raise self.Conflict('Document update conflict')
            elif not self.contains(idx, rev):
                raise self.Conflict('Document update conflict')
        elif rev is not None:
            raise self.Conflict('Document update conflict')

    def contains(self, idx, rev=None):
        if idx not in self._docs:
            return False
        doc = self._docs[idx]
        if rev is None:
            return not doc.get('_deleted', False)
        return self._docs[idx]['_rev'] == rev

    def load(self, idx, rev=None):
        if not self.contains(idx, rev):
            raise self.NotFound(idx)
        return self._docs[idx]

    def store(self, doc, rev=None, new_edits=True):
        if '_id' not in doc:
            doc['_id'] = str(uuid.uuid4()).lower()
        if rev is None:
            rev = doc.get('_rev')

        idx = doc['_id']

        if new_edits:
            self.check_for_conflicts(idx, rev)
            doc['_rev'] = self._new_rev(doc)
        else:
            assert rev, 'Document revision missed'
            doc['_rev'] = rev

        idx, rev = doc['_id'], doc['_rev']

        self._docs[idx] = doc
        self._update_seq += 1
        self._changes[idx] = self._update_seq

        return idx, rev

    def remove(self, idx, rev):
        if not self.contains(idx):
            raise self.NotFound(idx)
        elif not self.contains(idx, rev):
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
            if not self.contains(idx):
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
            try:
                idx, rev = self.store(doc, None, new_edits)
                res.append({
                    'ok': True,
                    'id': idx,
                    'rev': rev
                })
            except Exception as err:
                res.append({'id': doc.get('_id'),
                            'error': type(err).__name__,
                            'reason': str(err)})
        return res

    def ensure_full_commit(self):
        return {
            'ok': True,
            'instance_start_time': self.info()['instance_start_time']
        }

    def changes(self, since=0, feed='normal', style='all_docs', filter=None):
        changes = sorted(self._changes.items(), key=lambda i: i[1])
        if since:
            for idx, seq in changes:
                if since <= seq:
                    yield self.make_event(idx, seq)
                    break
        for idx, seq in changes:
            yield self.make_event(idx, seq)

    def add_attachment(self, doc, name, data, ctype='application/octet-stream'):
        atts = doc.setdefault('_attachments')
        digest = 'md5-%s' % base64.b64encode(hashlib.md5(data).digest()).decode()
        if doc.get('_rev'):
            revpos = int(doc['_rev'].split('-')[0]) + 1
        else:
            revpos = 1
        atts[name] = {
            'data': data,
            'digest': digest,
            'length': len(data),
            'content_type': ctype,
            'revpos': revpos
        }

    def make_event(self, idx, seq):
        doc = self._docs[idx]
        event = {
            'id': idx,
            'changes': [{'rev': doc['_rev']}],
            'seq': seq
        }
        if doc.get('_deleted'):
            event['_deleted'] = True
        return event
