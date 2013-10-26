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

    def test_multipart_related(self):
        data = (b'--abc\r\n'
                b'Content-Type: application/json\r\n\r\n'
                b'{"foo":"bar",'
                b'"_attachments":{"data.txt":{"content_type":"text/plain",'
                b'"revpos":4,"digest":"md5-IRgCvi7N+T8xYLHTUBwttg==",'
                b'"length":24,"follows":true}}}\r\n'
                b'--abc\r\n'
                b'Content-Disposition: attachment; filename="data.txt"\r\n'
                b'Content-Type: text/plain\r\nContent-Length: 24\r\n\r\n'
                b'Replicate All The Data!\n\r\n'
                b'--abc--')
        rv = self.app.put('/%s/%s' % (self.dbname, self.docid),
                          data=data,
                          content_type='multipart/related;boundary=abc')
        assert rv.status_code == 201


class DesignDocsTestCase(DocumentAPITestCase):

    docid = '_design/abc'


class ReplicationLogTestCase(DocumentAPITestCase):

    docid = '_local/abc'


class RevsDiffTestCase(ReplipyDBTestCase):

    def setUp(self):
        super(RevsDiffTestCase, self).setUp()
        rv = self.app.put('/%s/%s' % (self.dbname, 'doc'),
                          data=self.encode({'foo': 'bar'}),
                          content_type='application/json')
        resp = self.decode(rv)
        rv = self.app.put('/%s/%s' % (self.dbname, 'doc'),
                          data=self.encode({'foo': 'bar', '_rev': resp['rev']}),
                          content_type='application/json')
        resp = self.decode(rv)
        self.idrev = resp['id'], resp['rev']

    def test_no_missing(self):
        idx, rev = self.idrev
        data = {idx: [rev]}

        rv = self.app.post('/%s/_revs_diff' % self.dbname,
                           data=self.encode(data),
                           content_type='application/json')
        assert rv.status_code == 200

        resp = self.decode(rv)
        assert resp == {}

    def test_missing_revs(self):
        idx, rev = self.idrev
        data = {'foo': ['1-ABC', '2-CDE'], idx: [rev, '1-QWE']}

        rv = self.app.post('/%s/_revs_diff' % self.dbname,
                           data=self.encode(data),
                           content_type='application/json')
        assert rv.status_code == 200

        resp = self.decode(rv)
        assert resp['foo']['missing'] == ['1-ABC', '2-CDE']
        assert resp[idx]['missing'] == ['1-QWE']


class BulkDocsTestCase(ReplipyDBTestCase):

    def test_bulk_create(self):
        rv = self.app.post('/%s/_bulk_docs' % self.dbname,
                           data=self.encode({'docs': [{'_id': 'foo'},
                                                      {'_id': 'bar'}]}),
                           content_type='application/json')
        assert rv.status_code == 201

        resp = self.decode(rv)
        for res in resp:
            assert res['ok']
            assert res['id'] in ['foo', 'bar']
            assert 'rev' in res

    def test_bulk_update(self):
        docs = {
            'foo': {'_id': 'foo'},
            'bar': {'_id': 'bar'}
        }
        rv = self.app.post('/%s/_bulk_docs' % self.dbname,
                           data=self.encode({'docs': list(docs.values())}),
                           content_type='application/json')
        assert rv.status_code == 201

        resp = self.decode(rv)
        for res in resp:
            assert res['rev'].startswith('1-')
            docs[res['id']]['_rev'] = res['rev']

        rv = self.app.post('/%s/_bulk_docs' % self.dbname,
                           data=self.encode({'docs': list(docs.values())}),
                           content_type='application/json')
        assert rv.status_code == 201

        resp = self.decode(rv)
        for res in resp:
            assert res['ok']
            assert res['rev'].startswith('2-')

    def test_bulk_newedits(self):
        rv = self.app.post(
            '/%s/_bulk_docs' % self.dbname,
            data=self.encode({'docs': [{'_id': 'foo', '_rev': '9-X'},
                                       {'_id': 'bar', '_rev': '9-X'}],
                              'new_edits': False}),
            content_type='application/json')
        assert rv.status_code == 201

        resp = self.decode(rv)
        for res in resp:
            assert res['ok']
            assert res['id'] in ['foo', 'bar']
            assert res['rev'] == '9-X'


class EnsureFullCommitTestCase(ReplipyDBTestCase):

    def test_ensure_full_commit(self):
        rv = self.app.post('/%s/_ensure_full_commit' % self.dbname,
                           content_type='application/json')
        assert rv.status_code == 201

        resp = self.decode(rv)
        assert resp['ok']
        assert 'instance_start_time' in resp


if __name__ == '__main__':
    unittest.main()
