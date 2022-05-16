import fire
from util import logger


class Tracker(object):
    def start_tracking(self):
        logger.debug("Starting tracking")
        pass


if __name__ == "__main__":
    tracker = Tracker()
    fire.Fire(tracker)
