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


@replipy.route('/<dbname>/<docid>',
               methods=['HEAD', 'GET', 'PUT', 'DELETE'])
def document(dbname, docid):
    def head():
        return get()

    def get():
        if docid not in db:
            return replipy.make_response(404, {'error': 'not_found',
                                               'reason': docid})
        return replipy.make_response(200, db.load(docid))

    def put():
        doc = flask.request.get_json()
        doc['_id'] = docid
        try:
            idx, rev = db.store(doc, flask.request.args.get('rev'))
        except replipy.db_cls.Conflict:
            return replipy.make_response(409, {
                'error': 'conflict'
            })
        else:
            return replipy.make_response(201, {
                'ok': True,
                'id': idx,
                'rev': rev
            })

    def delete():
        idx, rev = db.remove(docid)
        return replipy.make_response(201, {
            'ok': True,
            'id': idx,
            'rev': rev
        })

    if dbname not in replipy.dbs:
        return replipy.make_response(404, {'error': 'not_found',
                                           'reason': dbname})
    db = replipy.dbs[dbname]
    return locals()[flask.request.method.lower()]()


@replipy.route('/<dbname>/_design/<docid>',
               methods=['HEAD', 'GET', 'PUT', 'DELETE'])
def design_document(dbname, docid):
    return document(dbname, '_design/' + docid)


@replipy.route('/<dbname>/_local/<docid>',
               methods=['GET', 'PUT', 'DELETE'])
def local_document(dbname, docid):
    return document(dbname, '_local/' + docid)


@replipy.route('/<dbname>/_revs_diff', methods=['POST'])
def database_revs_diff(dbname):
    if dbname not in replipy.dbs:
        return replipy.make_response(404, {'error': 'not_found',
                                           'reason': dbname})
    db = replipy.dbs[dbname]
    return replipy.make_response(200, db.revs_diff(flask.request.get_json()))


@replipy.route('/<dbname>/_bulk_docs', methods=['POST'])
def database_bulk_docs(dbname):
    if dbname not in replipy.dbs:
        return replipy.make_response(404, {'error': 'not_found',
                                           'reason': dbname})
    db = replipy.dbs[dbname]
    return replipy.make_response(201, db.bulk_docs(**flask.request.get_json()))


@replipy.route('/<dbname>/_ensure_full_commit', methods=['POST'])
def database_ensure_full_commit(dbname):
    if dbname not in replipy.dbs:
        return replipy.make_response(404, {'error': 'not_found',
                                           'reason': dbname})
    db = replipy.dbs[dbname]
    return replipy.make_response(201, db.ensure_full_commit())
