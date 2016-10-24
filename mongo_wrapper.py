#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
    import pymongo
except ImportError:
    print "pymongo module is required"

class MongoWrapperException(Exception):
    pass


class MongoWrapper(object):

    def __init__(self, dbname, coll=None, host='localhost', port=27017):
        """(str [,str] [,str] [,int]

        Get db name, optional collection, host and port, defaults to
        localhost/27017
        """
        self._client = self._connect(host, port)
        self._db = self._set_db(dbname) if dbname else None
        self._coll = self._set_coll(coll) if coll else None

    def _connect(self, host, port):
        """(str, int)

        Connects to the MongoDb
        """
        return pymongo.MongoClient(host, port)

    def _set_db(self, dbname):
        assert isinstance(self._client, pymongo.MongoClient)
        return self._client[dbname]

    def _set_coll(self, collection):
        if not self._db:
            raise MongoWrapperException("Set db first")
        return self._db[collection]

    @property.setter
    def set_db(self, dbname):
        self._get_db(dbname)

    @property.setter
    def set_collection(self, coll):
        self._set_coll(coll)

    @property
    def doc_count(self):
        return self._coll.count()

    def doc_insert(self, doc):
        """(list of dict|dict

        If  `doc` is a type dict insert_one() will be called, if `doc' is a
        list of dicts, insert_many() will be called
        """
        if not self._coll:
            raise MongoWrapperException("Set collection first")
        if isinstance(doc, dict):
            self._coll.insert_one(doc)
        elif isinstance(doc, list):
            self._coll.insert_many(doc)

    def doc_find(self, params={}, sort=None):
        """([dict]) -> list of dict

        :param params: query object
        :return: list of dict
        """
        cursor = self._coll.find(params)
        if sort:
            return cursor.sort(sort)
        return cursor


