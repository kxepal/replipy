# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

"""Test suite for case when Replipy acts as Target for replication process"""

import unittest
from replipy.tests import ReplipyTestCase, ReplipyDBTestCase


class VerifyPeersTestCase(ReplipyTestCase):

    def test_missed_db(self):
        rv = self.app.head('/replipy/')
        assert rv.status_code == 404

    def test_create_db(self):
        rv = self.app.put('/replipy/', content_type='application/json')
        assert rv.status_code == 201

        resp = self.decode(rv)
        assert 'ok' in resp

    def test_create_fails_on_existed_db(self):
        rv = self.app.put('/replipy/', content_type='application/json')
        assert rv.status_code == 201

        rv = self.app.put('/replipy/', content_type='application/json')
        assert rv.status_code == 412

        resp = self.decode(rv)
        assert resp['error'] == 'db_exists'


class GetPeersInfoTestCase(ReplipyTestCase):

    def test_get_missed_db(self):
        rv = self.app.get('/replipy/', content_type='application/json')
        assert rv.status_code == 404

        resp = self.decode(rv)
        assert resp['error'] == 'not_found'

    def test_get_db_info(self):
        rv = self.app.put('/replipy/', content_type='application/json')
        assert rv.status_code == 201

        rv = self.app.get('/replipy/', content_type='application/json')
        assert rv.status_code == 200

        resp = self.decode(rv)
        assert resp['db_name'] == 'replipy'
        assert resp['instance_start_time'].isdigit()
        assert len(resp['instance_start_time']) == 16
        assert resp['update_seq'] == 0


class DocumentAPITestCase(ReplipyDBTestCase):

    docid = 'abc'

    def test_get_missed_doc(self):
        rv = self.app.get('/%s/%s' % (self.dbname, self.docid),
                          content_type='application/json')
        assert rv.status_code == 404

        resp = self.decode(rv)
        assert resp['error'] == 'not_found'

    def test_create_doc(self):
        rv = self.app.put('/%s/%s' % (self.dbname, self.docid),
                          data=self.encode({'foo': 'bar'}),
                          content_type='application/json')
        assert rv.status_code == 201

        resp = self.decode(rv)
        assert resp['ok']
        assert resp['id'] == self.docid

    def test_get_doc(self):
        rv = self.app.put('/%s/%s' % (self.dbname, self.docid),
                          data=self.encode({'foo': 'bar'}),
                          content_type='application/json')
        assert rv.status_code == 201

        rv = self.app.get('/%s/%s' % (self.dbname, self.docid),
                          content_type='application/json')
        assert rv.status_code == 200

        resp = self.decode(rv)
        assert resp['_id'] == self.docid
        assert resp['foo'] == 'bar'

    def test_conflict(self):
        rv = self.app.put('/%s/%s' % (self.dbname, self.docid),
                          data=self.encode({'foo': 'bar'}),
                          content_type='application/json')
        assert rv.status_code == 201

        resp = self.decode(rv)
        rev = resp['rev']

        rv = self.app.put('/%s/%s' % (self.dbname, self.docid),
                          data=self.encode({'foo': 'bar'}),
                          content_type='application/json')
        assert rv.status_code == 409

        rv = self.app.put('/%s/%s' % (self.dbname, self.docid),
                          data=self.encode({'foo': 'bar', '_rev': rev}),
                          content_type='application/json')
        assert rv.status_code == 201, rv.data


class DesignDocsTestCase(DocumentAPITestCase):

    docid = '_design/abc'


class ReplicationLogTestCase(DocumentAPITestCase):

    docid = '_local/abc'


if __name__ == '__main__':
    unittest.main()
