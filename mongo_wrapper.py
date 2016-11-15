#!/usr/bin/env python
# -*- coding: utf-8 -*-

__doc__ = """Wrapper to the pymongo module"""

try:
    import pymongo
except ImportError:
    print "pymongo module is required"


class MongoWrapperException(Exception):
    pass

HOST = 'localhost'
PORT = 27017


def _get_connection(host, port):
    """
    Gets a mongodb connection

    :param host: host name (defaults to *localhost*)
    :param port: port number (defaults to 27017)
    :return: pymongo.MongoClient
    """
    if not host:
        host = HOST
    if not port:
        port = PORT
    return pymongo.MongoClient(host, port)


def _get_db(client, db):
    """
    Gets a database

    :param client: pymongo.MongoClient
    :param db: database name
    :return: pymongo.database object
    """
    return client[db]


def _get_coll(db, coll):
    """

    :param db: database name
    :param coll: document collection
    :return: a pymongo collection object
    """
    return db[coll]


def insert(doc, db, coll, host=None, port=None):
    """
    Inserts documents into a mongo collection

    If  `doc` is a type dict insert_one() will be called, if `doc` is a
    list of dicts, insert_many() will be called

    :param doc:
    :param db: database name
    :param coll: document collection
    :param host: host name (defaults to *localhost*)
    :param port: port number (defaults to 27017)
    :return:
    """
    _coll = _get_coll(_get_db(_get_connection(host, port), db), coll)
    if isinstance(doc, dict):
        _coll.insert_one(doc)
    elif isinstance(doc, list):
        _coll.insert_many(doc)


def find(db, coll, params={}, sort=None, host=None, port=None):
    """
    Finds documents in `coll` ection with matching `params`


    :param db: database name
    :param coll: document collection
    :param params: query object
    :param sort: if true the return value is sorted
    :param host: host name (defaults to *localhost*)
    :param port: port number (defaults to 27017)
    :return: list of documents found
    """
    cursor = _get_coll(_get_db(_get_connection(host, port), db), coll)
    if sort:
        return cursor.sort(sort)
    return cursor


def get_dblist(host=None, port=None):
    return _get_connection(host, port).database_names()

def get_coll_list(db, host=None, port=None):
    return _get_db(_get_connection(host, port), db).collection_names()

def drop_db(db, host=None, port=None):
    _get_connection(host, port).drop_database(db)

def drop_coll(coll, db, host=None, port=None):
    _get_db(_get_connection(host, port), db).drop_collection(coll)

def doc_count(db, coll, host=None, port=None):
     return _get_coll(_get_db(_get_connection(host, port), db), coll).count()



