# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import json
import flask
from .storage import ABCDatabase


class ReplicationBlueprint(flask.Blueprint):

    db_cls = ABCDatabase

    def __init__(self, *args, **kwargs):
        super(ReplicationBlueprint, self).__init__(*args, **kwargs)
        self._dbs = {}

    @property
    def dbs(self):
        return self._dbs

    def make_response(self, code, data):
        resp = flask.make_response(json.dumps(data))
        resp.status_code = code
        resp.headers['Content-Type'] = 'application/json'
        return resp


replipy = ReplicationBlueprint('replipy', __name__)


@replipy.route('/<dbname>/', methods=['HEAD', 'GET', 'PUT'])
def database(dbname):
    def head():
        return get()

    def get():
        if dbname not in replipy.dbs:
            return replipy.make_response(404, {'error': 'not_found',
                                               'reason': dbname})
        return replipy.make_response(200, replipy.dbs[dbname].info())

    def put():
        if dbname not in replipy.dbs:
            replipy.dbs[dbname] = replipy.db_cls(dbname)
            return replipy.make_response(201, {'ok': True})
        return replipy.make_response(412, {'error': 'db_exists'})

    return locals()[flask.request.method.lower()]()
