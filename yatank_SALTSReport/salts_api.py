# -*- coding: utf-8 -*-

from requests.auth import AuthBase
import slumber


CONFIG_ENCODING = "utf-8"


class TastypieApikeyAuth(AuthBase):
    def __init__(self, username, apikey):
        self.username = username
        self.apikey = apikey

    def __call__(self, r):
        r.headers['Authorization'] = "ApiKey {0}:{1}".format(self.username, self.apikey)
        return r


class DRFApikeyAuth(AuthBase):
    def __init__(self, username, apikey):
        self.username = username
        self.apikey = apikey

    def __call__(self, r):
        r.headers['Authorization'] = "Token {0}".format(self.apikey)
        return r


class SaltsApiException(Exception):
    pass


class SaltsApiClient(object):

    def __init__(self, url, user, key, api_type):
        try:
            if api_type == "DRF":
                self.api = slumber.API(url, auth=DRFApikeyAuth(user, key))
            else:
                self.api = slumber.API(url, auth=TastypieApikeyAuth(user, key))
            self.api.generatortype.get()
        except Exception, ex:
            raise SaltsApiException("Error connection with salts: %s" % ex)

    def store_result(self, **kwargs):
        gen_types = []
        if "generator_types" in kwargs:
            gen_types = kwargs["generator_types"]
        try:
            gen_type_objects = [self.api.generatortype.get(name=gt)[0]
                                for gt in gen_types]
            data = {"test_id": kwargs["test_id"],
                    "scenario_id": kwargs["scenario_id"],
                    "dt_start": kwargs["start"],
                    "dt_finish": kwargs["finish"],
                    "group": (kwargs["group"] or "").decode(CONFIG_ENCODING)[:32],
                    "test_name": (kwargs["test_name"] or "").decode(CONFIG_ENCODING)[:128],
                    "target":  kwargs["target"].decode(CONFIG_ENCODING)[:128],
                    "version":  (kwargs["version"] or "").decode(CONFIG_ENCODING)[:128],
                    "rps":  kwargs["rps"].decode(CONFIG_ENCODING)[:128],
                    "q99":  kwargs["q99"],
                    "q90":  kwargs["q90"],
                    "q50":  kwargs["q50"],
                    "http_errors_perc":  kwargs["http_errors_perc"],
                    "net_errors_perc":  kwargs["net_errors_perc"],
                    "graph_url":  kwargs["graph_url"].decode(CONFIG_ENCODING)[:256],
                    "generator":  kwargs["host"].decode(CONFIG_ENCODING)[:128],
                    "user":  kwargs["user"].decode(CONFIG_ENCODING)[:128],
                    "ticket_id":  (kwargs["ticket_id"] or "").decode(CONFIG_ENCODING)[:64],
                    "mnt_url":  (kwargs["mnt_url"] or "").decode(CONFIG_ENCODING)[:256],
                    "comments": "",
                    "generator_types": gen_type_objects,
                    "test_status": kwargs["test_status"]}
            res = self.api.testresult.post(data)
            return res
        except Exception, ex:
            raise SaltsApiException("Error sending results to salts: %s" % ex)

    def store_file(self, file_path, id, field):
        try:
            with open(file_path) as fp:
                self.api.testresult(id).put(files={field: fp})
        except Exception, ex:
            raise SaltsApiException("Error sending artifact file to salts: %s" % ex)
