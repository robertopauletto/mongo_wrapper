import unittest
import mongo_wrapper as mw


class TestMongoWrapper(unittest.TestCase):

    def setUp(self):
        self._diz = [
            {'nome': 'nome', 'cognome': 'cognome', 'anni': 32},
            {'nome': 'nome1', 'cognome': 'cognome1', 'anni': 42},
        ]
        self._testdb = 'testdb'
        self._testcoll = 'testcoll'
        mw.insert(self._diz, self._testdb, self._testcoll)

    def tearDown(self):
        dbnames = mw.get_dblist()
        if self._testdb in dbnames:
            mw.drop_db(self._testdb)

    def _raise_mongow_exp(self):
        raise mw.MongoWrapperException("Mongo Exception!")

    def test_custom_exception(self):
        with self.assertRaises(mw.MongoWrapperException):
            self._raise_mongow_exp()

    def test_insert_many(self):
        to_ins = [
            {'nome': 'mario', 'cognome': 'rossi', 'anni': 32},
            {'nome': 'ugo', 'cognome': 'verdi', 'anni': 42},
        ]
        x = mw.doc_count(self._testdb, self._testcoll)
        mw.insert(to_ins, self._testdb, self._testcoll)
        self.assertEqual(x+2,mw.doc_count(self._testdb, self._testcoll),  x)

    def test_insert_one(self):
        x = mw.doc_count(self._testdb, self._testcoll)
        mw.insert(
            {'nome': 'nome3', 'cognome': 'cognome3', 'anni': 132},
            self._testdb, self._testcoll
        )
        self.assertEqual(x+1, mw.doc_count(self._testdb, self._testcoll), x)

    def test_drop_collection(self):
        coll_list = mw.get_coll_list(self._testdb)
        if self._testcoll in coll_list:
            mw.drop_coll(self._testcoll, self._testdb)
            coll_list = mw.get_coll_list(self._testdb)
        self.assertTrue(self._testcoll not in coll_list)

    def test_drop_db(self):
        dbnames = mw.get_dblist()
        if self._testdb in dbnames:
            mw.drop_db(self._testdb)
            dbnames = mw.get_dblist()
        self.assertTrue(self._testdb not in dbnames)


if __name__ == '__main__':
    unittest.main()
