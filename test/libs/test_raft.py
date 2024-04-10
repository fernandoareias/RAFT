# tests/test_raft.py

import unittest
from src.libs.raft import Raft
from src.libs.core import RaftStatus

class TestRaft(unittest.TestCase):

    def test_should_create_raft(self):
        self.assertIsNotNone(Raft(1))

    def test_when_create_raft_should_have_node_id(self):
        raft = Raft(1)
        self.assertEqual(1, raft.node_id)

    def test_should_create_raft_with_state_FOLLOWER(self):
        raft = Raft(1)
        self.assertEqual(RaftStatus.FOLLOWER, raft.state)

if __name__ == '__main__':
    unittest.main()