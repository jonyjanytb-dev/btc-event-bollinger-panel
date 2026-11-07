"""A chart touch is not a scorable proxy candidate without a live index feed."""
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from chart_strategy import capture, open_db, require_healthy_index
from test_double_touch import RECEIVED, pair


class IndexReadinessTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / 'shadow.sqlite'
        self.health = sqlite3.connect(self.path)
        self.health.execute('CREATE TABLE health(source TEXT PRIMARY KEY,status TEXT,checked_ms INTEGER,detail TEXT)')
        self.health.commit()

    def tearDown(self):
        self.health.close()
        self.directory.cleanup()

    def set_index(self, status, checked_ms, event_ms):
        self.health.execute('INSERT OR REPLACE INTO health VALUES(?,?,?,?)',
                            ('index',status,checked_ms,json.dumps({'event_ms':event_ms})))
        self.health.commit()

    def test_fresh_live_index_is_accepted(self):
        self.set_index('OK', RECEIVED-300, RECEIVED-600)
        require_healthy_index(self.path, RECEIVED, 5_000)

    def test_disconnected_and_stale_index_are_rejected(self):
        for status,checked,event in (
            ('DISCONNECTED',RECEIVED-300,RECEIVED-600),
            ('OK',RECEIVED-6_000,RECEIVED-6_100),
            ('OK',RECEIVED-300,RECEIVED-6_000),
        ):
            with self.subTest(status=status,checked=checked,event=event):
                self.set_index(status,checked,event)
                with self.assertRaisesRegex(ValueError,'INDEX_UNAVAILABLE'):
                    require_healthy_index(self.path, RECEIVED, 5_000)

    def test_no_index_health_is_rejected(self):
        with self.assertRaisesRegex(ValueError,'INDEX_UNAVAILABLE'):
            require_healthy_index(self.path, RECEIVED, 5_000)

    def test_capture_does_not_record_signal_when_index_is_down(self):
        class Client:
            def snapshot(self):return pair(),RECEIVED
        observed = open_db(Path(self.directory.name) / 'observations.sqlite')
        try:
            self.set_index('DISCONNECTED',RECEIVED-100,RECEIVED-200)
            with self.assertRaisesRegex(ValueError,'INDEX_UNAVAILABLE'):
                capture(Client(),observed,lambda now:require_healthy_index(self.path,now,5_000))
            self.assertEqual(observed.execute('SELECT COUNT(*) FROM observations').fetchone()[0],0)
        finally:observed.close()


if __name__=='__main__':unittest.main()
