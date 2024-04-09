import unittest
from src.libs.raft import Raft

class TestRaft(unittest.TestCase):

    def test_should_create_raft(self):
        self.assertIsNotNone(Raft())