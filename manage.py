import sys

from lgsf.storage.backends import get_storage_backend

sys.path.append("lgsf")
sys.path.append("scrapers")


if __name__ == "__main__":
    from lgsf import runner

    runner.CommandRunner(sys.argv)
