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
import werkzeug.exceptions
import werkzeug.http
from flask import current_app as app
from .storage import ABCDatabase


replipy = flask.Blueprint('replipy', __name__)


def make_response(code, data):
    resp = flask.make_response(json.dumps(data))
    resp.status_code = code
    resp.headers['Content-Type'] = 'application/json'
    return resp


def make_error_response(code, error, reason):
    if isinstance(reason, werkzeug.exceptions.HTTPException):
        reason = reason.description
    else:
        reason = str(reason)
    return make_response(code, {'error': error, 'reason': reason})


def database_should_exists(func):
    @functools.wraps(func)
    def check_db(dbname, *args, **kwargs):
        if dbname not in app.dbs:
            return flask.abort(404, '%s missed' % dbname)
        return func(dbname, *args, **kwargs)
    return check_db


@replipy.record_once
def setup(state):
    state.app.db_cls = state.options.get('db_cls', ABCDatabase)
    state.app.dbs = {}


@replipy.errorhandler(400)
def bad_request(err):
    return make_error_response(400, 'bad_request', err)


@replipy.errorhandler(404)
@replipy.errorhandler(ABCDatabase.NotFound)
def not_found(err):
    return make_error_response(404, 'not_found', err)


@replipy.errorhandler(409)
@replipy.errorhandler(ABCDatabase.Conflict)
def conflict(err):
    return make_error_response(409, 'conflict', err)


@replipy.errorhandler(412)
def db_exists(err):
    return make_error_response(412, 'db_exists', err)


@replipy.route('/<dbname>/', methods=['HEAD', 'GET', 'PUT'])
def database(dbname):
    def head():
        return get()

    def get():
        if dbname not in app.dbs:
            return flask.abort(404, '%s missed' % dbname)
        return make_response(200, app.dbs[dbname].info())

    def put():
        if dbname in app.dbs:
            return flask.abort(412, dbname)
        app.dbs[dbname] = app.db_cls(dbname)
        return make_response(201, {'ok': True})

    return locals()[flask.request.method.lower()]()


@replipy.route('/<dbname>/<docid>', methods=['HEAD', 'GET', 'PUT', 'DELETE'])
@database_should_exists
def document(dbname, docid):
    def head():
        return get()

    def get():
        doc = db.load(docid, flask.request.args.get('rev', None))
        return make_response(200, doc)

    def put():
        rev = flask.request.args.get('rev')
        new_edits = json.loads(flask.request.args.get('new_edits', 'true'))

        if flask.request.mimetype == 'application/json':
            doc = flask.request.get_json()

        elif flask.request.mimetype == 'multipart/related':
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
            return flask.abort(400)

        doc['_id'] = docid

        idx, rev = db.store(doc, rev, new_edits)
        return make_response(201, {'ok': True, 'id': idx, 'rev': rev})

    def delete():
        idx, rev = db.remove(docid, flask.request.args.get('rev', None))
        return make_response(201, {'ok': True, 'id': idx, 'rev': rev})

    db = app.dbs[dbname]
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
    db = app.dbs[dbname]
    return make_response(200, db.revs_diff(flask.request.get_json()))


@replipy.route('/<dbname>/_bulk_docs', methods=['POST'])
@database_should_exists
def database_bulk_docs(dbname):
    db = app.dbs[dbname]
    return make_response(201, db.bulk_docs(**flask.request.get_json()))


@replipy.route('/<dbname>/_ensure_full_commit', methods=['POST'])
@database_should_exists
def database_ensure_full_commit(dbname):
    db = app.dbs[dbname]
    return make_response(201, db.ensure_full_commit())


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
    db = app.dbs[dbname]
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
