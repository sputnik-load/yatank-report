# -*- coding: utf-8 -*-

import unittest
import logging
import psycopg2
import re
from version import UNKNOWN_VERSION, target_version, sips_check

format_str = "%(asctime)s: [%(levelname)s]: %(message)s"
logging.basicConfig(level=logging.INFO, format=format_str)
log = logging.getLogger("test_version")
fh = logging.FileHandler("test_version.log")
fh.setLevel(logging.INFO)
fh.setFormatter(logging.Formatter(format_str))
log.addHandler(fh)


class TestVersion(unittest.TestCase):

    def __init__(self, methodName):
        unittest.TestCase.__init__(self, methodName)
        conn_string_templ = "host='%s' dbname='%s' user='%s' password='%s'"
        # settings = ("localhost", "test_salts_db", "salts", "salts")
        settings = ("salt-dev", "salts", "salts", "salts")
        self.conn = psycopg2.connect(conn_string_templ % settings)
        pattern = re.compile("[^\W\d_][\w\-\:]+")
        self.targets = []
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT DISTINCT target FROM salts_testresult")
            results = cursor.fetchall()
            self.targets = [r[0] for r in results if r[0] and pattern.match(r[0])]


    def setUp(self):
        pass

    def test_frontend(self):
        (v, message) = target_version("<FRONT_HOST>:<FRON_PORT>", "/")
        log.info(message)
        self.assertNotEqual(v, UNKNOWN_VERSION)

    def test_common_service(self):
        (v, message) = target_version("<TEST-SERVICE_HOST>:<TEST-SERVICE_PORT>", "/")
        log.info(message)
        self.assertEqual(v, "2.3.6 r8907445")

    def test_special_service(self):
        (v, message) = target_version("<TARGET_HOST>:<TARGET_PORT>", "<QUERY>")
        log.info(message)
        self.assertNotEqual(v, UNKNOWN_VERSION)

    def test_sips_check(self):
        package = "sips_some_service-(.*?)-(r.*?)\.suffix\.x86_64\.rpm"
        repo = "http://<REPOHOST>/<REPONAME>-x86_64/"
        ver = "3.16.0 r214630"
        (sips_err, message) = sips_check(package, repo, ver)
        log.info(message)
        self.assertTrue(sips_err, "Version and revision are not matched.")

        ver = "3.1.0 r214630"
        (sips_err, message) = sips_check(package, repo, ver)
        log.warning(message)
        self.assertFalse(sips_err, "Version should be not matched.")

        ver = "3.16.0 r214631"
        (sips_err, message) = sips_check(package, repo, ver)
        log.warning(message)
        self.assertFalse(sips_err, "Version should be not matched.")

    def test_services(self):
        versions = {"fe": ("/", []),
                    "one_service": ("/node_one", []),
                    "another_service": ("/another_node", []),
                    "yet_another_service": ("/yet/another/node/q?q=test", [])}
        ignore_targets = ["ignore_prefix_1_*", "ignore_prefix_2_*", "ignore_hostname"]
        unk_services = []
        (unknown, known, ignore_n) = (0, 0, 0)
        for target in self.targets:
            ignore = False
            for ign_target in ignore_targets:
                pat = re.compile(ign_target)
                if pat.match(target):
                    ignore = True
                    break
            if ignore:
                ignore_n += 1
                continue
            target_report = []
            ver = None
            for service_type in versions:
                (path, ver_list) = versions[service_type]
                (ver, message) = target_version(target, path)
                if ver == UNKNOWN_VERSION:
                    target_report.append((target, message, path))
                else:
                    ver_list.append((target, ver))
                    known += 1
                    break
            if ver == UNKNOWN_VERSION:
                unk_services += target_report
                unknown += 1
        for service_type in versions:
            ver_list = versions[service_type][1]
            for item in ver_list:
                (target, version) = item
                log.info("Service Type: %s. URL: %s. Version: %s." % (service_type, target, version))
        for (url, message, path) in unk_services:
            log.warning("URL: %s. Path: %s. Message: %s." % (url, path, message))
        log.info("Total: %d. Known: %d. Unknown: %d. Ignore: %d." % (len(self.targets),
                                                                     known, unknown, ignore_n))
        self.assertFalse(unk_services, "Not all service versions are defined.")

    def tearDown(self):
        pass


if __name__ == "__main__":
    unittest.main()
