"""CI entry for cluster_smoke RESP helpers. Does not start a server."""
import unittest

from cluster_smoke import RespHelperTests

if __name__ == "__main__":
    unittest.main(verbosity=2)
