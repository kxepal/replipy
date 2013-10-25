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
from replipy.tests import ReplipyTestCase


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


if __name__ == '__main__':
    unittest.main()
