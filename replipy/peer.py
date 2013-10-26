# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import functools
import json
import flask
import werkzeug.http
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


def database_should_exists(func):
    @functools.wraps(func)
    def check_db(dbname, *args, **kwargs):
        if dbname not in replipy.dbs:
            return replipy.make_response(404, {'error': 'not_found',
                                               'reason': dbname})
        return func(dbname, *args, **kwargs)
    return check_db


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


@replipy.route('/<dbname>/<docid>', methods=['HEAD', 'GET', 'PUT', 'DELETE'])
@database_should_exists
def document(dbname, docid):
    def head():
        return get()

    def get():
        try:
            doc = db.load(docid, flask.request.args.get('rev', None))
        except db.NotFound as err:
            return replipy.make_response(404, {'error': 'not_found',
                                               'reason': docid})
        else:
            return replipy.make_response(200, doc)

    def put():
        rev = flask.request.args.get('rev')
        new_edits = json.loads(flask.request.args.get('new_edits', 'true'))

        if flask.request.content_type.startswith('application/json'):
            doc = flask.request.get_json()

        elif flask.request.content_type.startswith('multipart/related'):
            parts = parse_multipart_data(
                flask.request.stream, flask.request.mimetype_params['boundary'])

            # CouchDB has an agreement, that document goes before attachments
            # which simplifies processing logic and reduces footprint
            headers, body = next(parts)
            assert headers['Content-Type'] == 'application/json'
            doc = json.loads(body.decode())
            # We have to inject revision into doc there to correct compute
            # revpos field for attachments
            doc.setdefault('_rev', rev)

            for headers, body in parts:
                params = werkzeug.http.parse_options_header(
                    headers['Content-Disposition'])[1]
                fname = params['filename']
                ctype = headers['Content-Type']
                db.add_attachment(doc, fname, body, ctype)

        else:
            # mimics to CouchDB response in case of unsupported mime-type
            return replipy.make_response(400, {
                'error': 'bad_request',
                'reason': 'invalid_json'
            })

        doc['_id'] = docid

        try:
            idx, rev = db.store(doc, rev, new_edits)
        except replipy.db_cls.Conflict as err:
            return replipy.make_response(409, {
                'error': 'conflict',
                'reason': str(err)
            })
        else:
            return replipy.make_response(201, {
                'ok': True,
                'id': idx,
                'rev': rev
            })

    def delete():
        try:
            idx, rev = db.remove(docid, flask.request.args.get('rev', None))
        except db.NotFound as err:
            return replipy.make_response(404, {'error': 'not_found',
                                               'reason': docid})
        except db.Conflict as err:
            return replipy.make_response(409, {
                'error': 'conflict',
                'reason': str(err)
            })
        else:
            return replipy.make_response(201, {
                'ok': True,
                'id': idx,
                'rev': rev
            })

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
@database_should_exists
def database_revs_diff(dbname):
    db = replipy.dbs[dbname]
    return replipy.make_response(200, db.revs_diff(flask.request.get_json()))


@replipy.route('/<dbname>/_bulk_docs', methods=['POST'])
@database_should_exists
def database_bulk_docs(dbname):
    db = replipy.dbs[dbname]
    return replipy.make_response(201, db.bulk_docs(**flask.request.get_json()))


@replipy.route('/<dbname>/_ensure_full_commit', methods=['POST'])
@database_should_exists
def database_ensure_full_commit(dbname):
    db = replipy.dbs[dbname]
    return replipy.make_response(201, db.ensure_full_commit())


@replipy.route('/<dbname>/_changes', methods=['GET'])
@database_should_exists
def database_changes(dbname):
    def generator(changes, last_seq):
        yield '{"last_seq": %d,' % last_seq
        yield '"results":['
        change = next(changes)
        yield json.dumps(change)
        for change in changes:
            yield ',' + json.dumps(change)
        yield ']}'
    db = replipy.dbs[dbname]
    last_seq = db.update_seq

    args = flask.request.args
    heartbeat = args.get('heartbeat', 10000)
    since = json.loads(args.get('since', '0'))
    feed = args.get('feed', 'normal')
    style = args.get('style', 'all_docs')
    filter = args.get('filter', None)

    changes = db.changes(since, feed, style, filter)
    return flask.Response(generator(changes, last_seq),
                          content_type='application/json')


def parse_multipart_data(stream, boundary):
    boundary = boundary.encode()
    next_boundary = boundary and b'--' + boundary or None
    last_boundary = boundary and b'--' + boundary + b'--' or None

    stack = []

    state = 'boundary'
    line = next(stream).rstrip()
    assert line == next_boundary
    for line in stream:
        if line.rstrip() == last_boundary:
            break

        if state == 'boundary':
            state = 'headers'
            if stack:
                headers, body = stack.pop()
                yield headers, b''.join(body)
            stack.append(({}, []))

        if state == 'headers':
            if line == b'\r\n':
                state = 'body'
                continue
            headers = stack[-1][0]
            line = line.decode()
            key, value = map(lambda i: i.strip(), line.split(':'))
            headers[key] = value

        if state == 'body':
            if line.rstrip() == next_boundary:
                state = 'boundary'
                continue
            stack[-1][1].append(line)

    if stack:
        headers, body = stack.pop()
        yield headers, b''.join(body)
