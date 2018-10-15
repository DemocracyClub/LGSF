import sys

sys.path.append("lgsf")
sys.path.append("scrapers")


if __name__ == "__main__":
    from lgsf import runner

    runner.CommandRunner(sys.argv)
