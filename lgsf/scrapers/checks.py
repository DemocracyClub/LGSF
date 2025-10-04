"""
Checks that scrapers look as expected.
"""

import abc


class ScraperChecker(object):
    def __init__(self, scraper_class, checks=None, output_errors_only=True):
        """
        A class for checking the quality of a Scraper Class.

        :param output_errors_only: (optional) Boolean.
        :param scraper_class: A :class:`ScraperBase` subclass.

        """
        self.scraper_class = scraper_class
        self.output_errors_only = output_errors_only
        if checks:
            self.checks = checks
        else:
            # Default checks
            self.checks = ()

    def run_checks(self):
        """
        Runs checks against a scraper class.
        """
        errors = 0
        for check_class in self.checks:
            if check_class(self.scraper_class).run_check():
                errors += 1


class baseCheck(metaclass=abc.ABCMeta):
    def __init__(self, scraper_class, output_errors_only=True):
        self.scraper_class = scraper_class
        self.output_errors_only = output_errors_only

    @abc.abstractmethod
    def check(self):
        pass

    def run_check(self):
        msg = "{}: ".format(self.name)
        error = False
        try:
            self.check()
            if not self.output_errors_only:
                msg += "✔"
        except Exception as e:
            msg += "✘ {}".format(e)
            error = True
        if not self.output_errors_only or error:
            print(msg)
        return error
