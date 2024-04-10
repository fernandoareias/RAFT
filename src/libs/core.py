from enum import Enum 

class RaftStatus(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3
