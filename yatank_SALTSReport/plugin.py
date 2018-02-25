# -*- coding: utf-8 -*-
"""Report plugin that plots some graphs"""

import sys
import datetime
import time
import tempfile
import json
import os
from collections import defaultdict
import getpass
import socket
import re
import pytz
from logging import Formatter, WARNING
import pkg_resources
import glob
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.header import Header
import slumber
from optparse import OptionParser
from urllib import quote
import importlib

from yandextank.plugins.Aggregator import AggregateResultListener, AggregatorPlugin
from yandextank.plugins.Monitoring import MonitoringPlugin
from yandextank.plugins.Monitoring.collector import MonitoringDataListener, MonitoringDataDecoder
from yandextank.core.tankcore import AbstractPlugin
from yandextank.plugins import Codes
from yandextank.plugins.Phantom import PhantomPlugin
from yandextank.plugins.JMeter import JMeterPlugin
from yandextank.plugins.bfg import bfgPlugin
from yandextank.plugins.GraphiteUploader import GraphiteUploaderPlugin
from version import UNKNOWN_VERSION, HAS_WARNING, target_version, sips_check
from report_handler import ReportHandler
from tank_api_client import PeaksCount


reload(sys)
sys.setdefaultencoding("utf-8")


METRICS_FILE_SUFFIX = ".txt"
METRICS_FILE_PREFIX = "salts_metrics_"
TXT_REPORT_SUFFIX = ".txt"
TXT_REPORT_PREFIX = "salts_reports_"
JSON_SECTION = "salts_timeouts"
READING_TIME = 5 # seconds TESTING-2042
MAX_FILE_SIZE = 75000000


def packages_to_tuples(packages):
    tuples = []
    for name in packages:
        tuples.append((name, packages[name]))
    return tuples


def generate_salts_report(headers, tuples, reptype):
    report = ''
    col_width = [0] * len(headers)
    header_sep = '|'
    jira_correct = 0
    if reptype == 'jira':
        header_sep = '||'
        jira_correct = 1
    for line in tuples:
        i = 0
        for value in line:
            col_width[i] = max(len(value), col_width[i])
            i += 1
    i = 0
    header_line = ''
    dash_line = ''
    for name in headers:
        col_width[i] = max(len(name), col_width[i])
        header_line += "%s %s " % (header_sep,
                                   name.center(col_width[i] - jira_correct))
        i += 1
    header_line += header_sep

    templ_item = " | %s"
    templ_end = " |\n"
    if reptype == 'raw':
        dash_line = '-' * len(header_line)
        report += dash_line + '\n'
        templ_item = "| %s "
        templ_end = "|\n"
    report += header_line + '\n'
    if reptype == "raw":
        report += dash_line + '\n'
    for line in tuples:
        i = 0
        for value in line:
            report += templ_item % value.center(col_width[i])
            i += 1
        report += templ_end
        if reptype == 'raw':
            report += dash_line + '\n'
    return report


def provide_package_versions():
    packages = {}
    for p in list(pkg_resources.Environment()):
        ver = pkg_resources.get_distribution(p).version
        packages[p] = ver
    return packages


def full_hostname():
    import subprocess
    r = subprocess.check_output(["hostname", "--all-fqdns"])
    return r.strip()


class SALTSReportPlugin(AbstractPlugin, AggregateResultListener,
                          MonitoringDataListener):
    """SALTS report"""

    SECTION = 'salts_report'
    DATEFORMAT = "%Y-%m-%d %H:%M:%S"
    EMAIL_ENCODING = 'utf-8'
    JIRA_URL = ''
    DEFAULT_MAIL_FROM = "ltbot@<LT_DOMAIN>"
    DEFAULT_GRAFANA_DASHBOARD_URL = "http://{grafana_host}/#/dashboard/db/" \
                                    "tankresultstpl?var-system={system}&" \
                                    "var-environment={env}&" \
                                    "var-collector={target_host}"
    TIMEZONE = 'Europe/Moscow'
    GRAPHITE_COMMON_PREFIX = 'load-tests'
    EMAIL_PACKAGES = "yandextank,yandex-tank-api,yandex-tank-api-client," \
                     "yatank-online,yatank-report,yatank-sputnikonline," \
                     "runload"
    DEFAULT_VERSION_URL_PATH = '/'

    @staticmethod
    def get_key():
        return __file__

    def __init__(self, core):
        AbstractPlugin.__init__(self, core)

        configure_dict = {'load_gen': self.get_load_gen,
                          'session_id': self.get_session_id,
                          'description': self.get_description,
                         }
        prepare_test_dict = {'ticket_id': self.get_ticket_id,
                             'ticket_url': self.get_ticket_url,
                             'gen_type': self.get_gen_type,
                             'user_name': self.get_user_name
                            }
        start_test_dict = {'start_time': self.get_time,
                           'planned_duration': self.get_planned_duration,
                           'rps': self.get_rps,
                           'rps_schedule': self.get_rps_schedule,
                           'start_graph_url': [self.get_graph_url, 'start']
                          }
        end_test_dict = {'end_time': self.get_time,
                         'duration': self.get_duration
                        }
        post_process_dict = {'summary': self.get_summary,
                             'jira_summary': self.get_jira_summary,
                             'short_summary': self.get_short_summary,
                             'warnings': self.get_warnings,
                             'q99': [self.get_quantile, 99.0],
                             'q90': [self.get_quantile, 90.0],
                             'q50': [self.get_quantile, 50.0],
                             'q100': [self.get_quantile, 100.0],
                             'cumulative_quantiles': \
                                self.get_cumulative_quantiles,
                             'net_perc_str': self.get_net_perc,
                             'http_perc_str': self.get_http_perc,
                             'net_info': self.get_net_info,
                             'http_info': self.get_http_info,
                             'http_net': self.get_http_net,
                             'end_graph_url': [self.get_graph_url, 'end'],
                             'http_codes_report': self.get_http_codes_report,
                             'net_codes_report': self.get_net_codes_report,
                             'edit_results_url': self.get_edit_results_url,
                             'http_codes_aggreg_report': \
                                self.get_http_codes_aggreg_report,
                             'jira_peaks_report': [self.get_peaks_report,
                                                   'jira'],
                             'raw_peaks_report': [self.get_peaks_report,
                                                  'raw'],
                             'jira_pack_versions': \
                                [self.get_versions_report,
                                 'jira', 'packages'],
                             'raw_pack_versions': \
                                [self.get_versions_report,
                                 'raw', 'packages'],
                             'jira_serv_versions': \
                                [self.get_versions_report,
                                 'raw', 'services'],
                             'raw_serv_versions': \
                                [self.get_versions_report,
                                 'raw', 'services'],
                            }

        self.order = ['configure', 'prepare_test',
                      'start_test', 'end_test',
                      'post_process']
        self.rules = {'configure': configure_dict,
                      'prepare_test': prepare_test_dict,
                      'start_test': start_test_dict,
                      'end_test': end_test_dict,
                      'post_process': post_process_dict
                     }
        self.data = {'hipchat_message_color': 'yellow',
                     'force_run': 0,
                     'yandex_api_server': hasattr(self.core, 'tank_worker'),
                    }
        self.peaks = None

        self.report_handler = ReportHandler(WARNING)
        self.report_handler.setFormatter(Formatter("%(asctime)s: %(message)s"))
        self.log.addHandler(self.report_handler)

        self.decoder = MonitoringDataDecoder()
        self.mon_data = {}

        def create_storage():
            return {
                'avg': defaultdict(list),
                'quantiles': defaultdict(list),
                'threads': {
                    'active_threads': []
                },
                'rps': {
                    'RPS': []
                },
                'http_codes': defaultdict(list),
                'net_codes': defaultdict(list),
            }

        def create_storage_cumulative():
            return {
                'avg': defaultdict(list),
                'quantiles': defaultdict(list),
            }

        self.overall = create_storage()
        self.cumulative = create_storage_cumulative()
        self.cases = defaultdict(create_storage)
        self.start_time = None
        self.end_time = None
        self.start_time_dt = None
        self.end_time_dt = None
        self.mail_from = None
        self.mail_to = None
        self.save_metrics_dump = None
        self.http_error_codes = None
        self.ticket_url = None
        self.grafana_dashboard_url = None
        self.api_url = None
        self.api_user = None
        self.api_key = None
        self.ticket_id = None
        self.generator_types = None
        self.rps_report = None
        self.session_id = None
        self.is_result_save = True
        self.generate_session_id()
        self.log.info("Test ID: %s" % self.session_id)
        self.new_id = None
        self.services = {}
        self.rps = {}
        self.rps_values = []
        self.salts_api_client = None
        self.shooting_status = 'P'
        self.user_name = ''
        self.packages = None
        self.email_packages = None
        self.salts_plugins = {'SputnikOnline': None,
                                'Salts': None}

    def _run_func(self, func, stage):
        if type(func) == list:
            arguments = list(func)
            func = arguments.pop(0)
            return func(stage, *arguments)
        else:
            return func(stage)

    def get_data(self, stage):
        for s in self.order:
            if self.rules.get(s):
                for param in self.rules[s]:
                    if not self.data.get(param):
                        self.data[param] = \
                            self._run_func(self.rules[s][param], stage)
            if s == stage:
                break
        return self.data

    def set_value(self, key, value):
        self.data[key] = value

    def calc_value(self, key, stage):
        if key in self.data:
            return self.data[key]
        for s in self.order:
            if self.rules.get(s):
                for param in self.rules[s]:
                    if param == key:
                        self.data[param] = \
                            self._run_func(self.rules[s][param], stage)
                        return self.data[param]
            if s == stage:
                break
        return None

    def get_time(self, stage):
        v = {'timestamp': int(time.time() + 0.5)}
        v['datetime'] = datetime.datetime.fromtimestamp(v['timestamp'])
        return v

    def get_summary(self, stage='post_process'):
        param = [('start_time', u'Дата'),
                 ('test_group', u'Группа'),
                 ('test_name', u'Тест'),
                 ('target', u'Мишень'),
                 ('system_version', u'Версия'),
                 ('rps', u'RPS'),
                 ('q99', '99%'),
                 ('q90', '90%'),
                 ('q50', '50%'),
                 ('http_net', u"%ошибок http/net"),
                 ('duration', u'Длительность'),
                 ('end_graph_url', u'Графики'),
                 ('load_gen', u'Генератор'),
                 ('session_id', u'ID сессии'),
                 ('ticket_url', u'Тикет URL'),
                 ('description', u'Описание'),
                 ('user_name', u'Пользователь'),
                 ('mnt_url', u'Методика НТ'),
                 ('ticket_id', u'Тикет ID'),
                 ('gen_type', u'Тип генератора'),
                ]
        summary = []
        for p in param:
            (key, title) = p
            value = self.calc_value(key, stage)
            summary.append({'key': key, 'title': title,
                            'value': value})
        return summary

    def get_jira_summary(self, stage='post_process'):
        param = [('test_name', u'Тест'),
                 ('target', u'Мишень'),
                 ('system_version', u'Версия'),
                 ('rps', u'RPS'),
                 ('q99', '99%'),
                 ('q90', '90%'),
                 ('q50', '50%'),
                 ('http_net', u"%ошибок http/net"),
                 ('duration', u'Длительность'),
                 ('end_graph_url', u'Графики'),
                 ('load_gen', u'Генератор'),
                 ('session_id', u'ID сессии'),
                 ('ticket_id', u'Тикет ID'),
                ]
        report = ''
        values_str = ''
        for p in param:
            (key, title) = p
            report += "||%s" % title
            value = self.calc_value(key, stage)
            if key == 'end_graph_url':
                values_str += "|[%s|%s]" % (title, value)
            else:
                values_str += "|%s" % value
        report = "%s||\n%s|" % (report, values_str)

        return report


    def get_rps(self, stage='start_test'):
        rps_values = []
        for r in self.rps:
            rps_line = self.rps[r]
            if r in ["phantom", "bfg"]:
                load_scheme = {"step": "step\(.*?(\d+),.*?(\d+),.*?\)",
                               "line": "line\(.*?(\d+),.*?(\d+),.*?\)",
                               "const": "const\(.*?(\d+),.*?\)"}
                rps_schedules = rps_line.split(";")
                for schedule in rps_schedules:
                    if schedule[0] == 'i':
			if not self.rps_values:
				return str(0)
			self.rps_values = sorted(self.rps_values)
                        m = len(self.rps_values)/2
                        if len(self.rps_values) % 2:
                            return str(self.rps_values[m])
                        else:
                            return str((self.rps_values[m-1] + self.rps_values[m])/2.0)
                    values = []
                    for load in load_scheme:
                        matches = re.findall(load_scheme[load], schedule)
                        if matches:
                            for m in matches:
                                values += list(m)
                    if values:
                        rps_values.append(max([int(v) for v in values]))
            elif r == "jmeter":
                rps_values.append(sum([int(r) for r in rps_line.split("..")]))
        return str(sum(rps_values))

    def get_cumulative_quantiles(self, stage='post_process'):
        def find_max(pair_values):
            _max = None
            for value in pair_values:
                if not _max or value[1] > _max:
                    _max = value[1]
            return _max

        def find_max_last(pair_values, last):
            _max = None
            for value in pair_values[-last:]:
                if not _max or value[1] > _max:
                    _max = value[1]
            return _max

        def find_last(pair_values):
            return pair_values[-1][1]

        cumulative_quantiles = {}
        try:
            for quantile, values in self.cumulative['quantiles'].items():
                cumulative_quantiles[quantile] = \
                    find_max_last(values, self.data['quantiles_fml'])
        except Exception as exc:
            self.log.warning("Exception on cumulative quantiles "
                             "obtaining: %s" % exc)

        return cumulative_quantiles

    def get_quantile(self, stage='post_process', *args):
        cumulative = self.calc_value('cumulative_quantiles', stage)
        if not cumulative:
            return '-'
        return cumulative.get(args[0], '-')

    def get_net_info(self, stage='post_process'):
        total = 0
        total_net_errors = 0
        net_codes_stat = {}  # массив с кол-вом по каждому NET-коду отдельно
        try:
            for net_code, values in self.overall['net_codes'].items():
                net_codes_stat[net_code] = sum(item[1] for item in values)
                total += net_codes_stat[net_code]

            if '0' in net_codes_stat:
                total_net_errors = total - net_codes_stat['0']
            else:
                total_net_errors = total
        except Exception as exc:
            self.log.warning("Exception on net info obtaining: %s" % exc)

        return (total, total_net_errors, net_codes_stat)

    def get_http_info(self, stage='post_process'):
        total_http_errors = 0
        http_codes_stat = {}  # массив с кол-вом по каждому HTTP-коду отдельно
        http_codes_aggreg_stat = {}  # массив с общей статистикой по
                                     # HTTP-кодам 2xx 3xx 4xx 5xx 6xx и т.д.
        try:
            for http_code, values in self.overall['http_codes'].items():
                http_code_xx = int(http_code) / 100
                if http_code_xx not in http_codes_aggreg_stat:
                    http_codes_aggreg_stat[http_code_xx] = 0
                http_codes_stat[http_code] = {'total': None}
                sub_total = sum(item[1] for item in values)
                http_codes_stat[http_code] = sub_total
                http_codes_aggreg_stat[http_code_xx] += sub_total
                if isinstance(http_code, basestring) \
                   and re.match(self.http_error_codes, http_code):
                    total_http_errors += sub_total
        except Exception as exc:
            self.log.warning("Exception on http info obtaining: %s" % exc)

        return (http_codes_stat, total_http_errors, http_codes_aggreg_stat)

    def get_net_perc(self, stage='post_process'):
        (total,
         total_net_errors,
         net_codes_stat) = self.calc_value('net_info', stage)
        net_perc_str = "0.00"
        if total:
            net_perc_str = "%.2f" % (float(total_net_errors) * 100 / total)
        return net_perc_str

    def get_http_perc(self, stage='post_process'):
        (total,
         total_net_errors,
         net_codes_stat) = self.calc_value('net_info', stage)
        (http_codes_stat,
         total_http_errors,
         http_codes_aggreg_stat) = self.calc_value('http_info', stage)
        http_perc_str = "0.00"
        if total:
            http_perc_str = "%.2f" % (float(total_http_errors) * 100 / total)
        return http_perc_str

    def get_http_net(self, stage='post_process'):
        http_perc_str = self.calc_value('http_perc_str', stage)
        net_perc_str = self.calc_value('net_perc_str', stage)
        return "%s / %s" % (http_perc_str, net_perc_str)

    def get_duration(self, stage='end_test'):
        start_time = self.data.get('start_time')
        end_time = self.data.get('end_time')
        if start_time and end_time:
            return str(end_time['datetime'] - start_time['datetime'])
        return ''

    def get_graph_url(self, stage='post_process', *args):

        def parse_graphite_prefix(line):
            parts = {}
            if not line:
                self.log.warning("Graphite Prefix: prefix value is empty.")
                return parts
            chunks = line.split(".")
            n_dots = len(chunks) - 1
            if n_dots == 0 or n_dots > 2:
                self.log.warning("Graphite Prefix: "
                                 "invalid format - no points or "
                                 "too many points, expected format "
                                 "'load-tests.<system>.<environment>'.")
                return parts
            if chunks[0] == self.GRAPHITE_COMMON_PREFIX:
                parts["common"] = self.GRAPHITE_COMMON_PREFIX
            else:
                self.log.warning("Graphite Prefix: invalid "
                                 "common name - '%s' value is expected." \
                                 % self.GRAPHITE_COMMON_PREFIX)
                return parts
            parts["system"] = chunks[1]
            if n_dots == 2:
                parts["env"] = chunks[2]
            else:
                self.log.warning("Graphite Prefix: deprecated format of "
                                 "graphite prefix - impossible to get "
                                 "'environment' value.")
            return parts

        def using_old_influx():
            if self.data['yandex_api_server']:
                config = self.core.tank_worker.core.config.config
                for s in config.sections():
                    if config.has_option(s, "old_influx"):
                        return True
            else:
                for s in self.core.config.config.sections():
                    if self.core.config.config.has_option(s, "old_influx"):
                        return True
            return False

        def format_datetime(dt):
            tz = pytz.timezone(self.TIMEZONE)
            utc = pytz.utc
            return tz.localize(dt).astimezone(utc).strftime('%Y%m%dT%H%M%S')

        graphite_prefix = ""
        try:
            graphite = self.core.get_plugin_of_type(GraphiteUploaderPlugin)
            graphite_prefix = self.core.get_option(graphite.SECTION,
                                                   'prefix', '')
            graphite_address = self.core.get_option(graphite.SECTION,
                                                    'address', '')
        except Exception, exc:
            self.log.info("Gprahite plugin disabled: %s" % exc)

        graph_url = ''
        try:
            target_host = self.data['target'].split(":")[0] \
                            or self.data['target']
            from_dt = None
            from_to_line = ''
            if args[0] == 'end':
                self.calc_value('start_time', stage)
                from_dt = self.data.get('start_time')['datetime'] - \
                            datetime.timedelta(seconds=5)
                to_dt = None
                to_dt = self.calc_value('end_time', stage)['datetime']
                from_dt -= datetime.timedelta(seconds=5)
                from_to_line = "&from=%s&to=%s" % (format_datetime(from_dt),
                                                   format_datetime(to_dt))
            if using_old_influx():
                self.log.warning("Old Influx is used.")
                if graphite_prefix:
                    prefix_parts = graphite_prefix.split(".")
                    if len(prefix_parts) == 1:
                        system = prefix_parts[0]
                    else:
                        system = prefix_parts[1]
                    graph_url = "%s?var-system=%s&var-collector=%s" \
                                % (self.grafana_dashboard_url, system,
                                   target_host)
                    graph_url += from_to_line
                    self.log.info("Graph URL: %s" % graph_url)
                else:
                    self.log.warning("Graphite Prefix is empty.")
            else:
                prefix = parse_graphite_prefix(graphite_prefix)
                if prefix:
                    graph_url = self.grafana_dashboard_url.replace(
                                    "{grafana_host}", graphite_address)
                    graph_url = graph_url.replace("{system}",
                                                  prefix["system"])
                    if "env" in prefix:
                        graph_url = graph_url.replace("{env}", prefix["env"])
                    graph_url = graph_url.replace("{target_host}",
                                                  target_host)
                    graph_url += from_to_line
                    self.log.info("Graph URL: %s" % graph_url)
        except Exception, exc:
            self.log.warning("Exception on graph url obtaining: %s" % exc)

        return graph_url

    def get_load_gen(self, stage='configure'):
        return full_hostname()

    def get_session_id(self, stage='confugure'):
        return self.session_id

    def get_ticket_id(self, stage='prepare_test'):
        if self.ticket_id == '':
            return self.ticket_id
        if self.ticket_id:
            if not re.match("^\w+-\d+$", self.ticket_id):
                self.log.warning("The 'ticket_id' option is not valid.")
                self.ticket_id = ''
            return self.ticket_id
        if self.ticket_url:
            self.log.warning("The 'ticket_url' option is deprecated. "
                             "Please use the 'ticket_id' option "
                             "in the scenario.")
            error_msg = "Ticket ID cannot be found in the ticket URL: %s" \
                        % self.ticket_url
            try:
                res = re.match('.*/([A-Z0-9\-]+)\?*', self.ticket_url)
                if res:
                    self.ticket_id = res.group(1)
                    if self.base_ticket_url:
                        self.log.warning("Ticket URL will be %s" % \
                                         self.base_ticket_url + self.ticket_id)
                else:
                    self.log.warning(error_msg)
                    self.ticket_id = ''
            except IndexError as exc:
                self.log.warning(error_msg)
                self.ticket_id = ''
        return self.ticket_id

    def get_ticket_url(self, stage='prepare_test'):
        ticket_id = self.calc_value('ticket_id', stage)
        ticket_url = ''
        if self.base_ticket_url and ticket_id:
            ticket_url = self.base_ticket_url + ticket_id
            self.log.info("Ticket URL to insert into email: %s" % ticket_url)
        else:
            self.log.warning("The 'base_ticket_url' option and/or "
                             "the 'ticket_id' option are absent. "
                             "Ticket URL is empty.")
        return ticket_url

    def get_description(self, stage='configure'):
        return '#comment#'

    def get_user_name(self, stage='start_test'):
        return self.user_name

    def get_gen_type(self, stage='prepare_test'):
        if self.generator_types:
            return " ".join(self.generator_types)

        if self.generator_types == []:
            return ""

        self.generator_types = []

        self.check_phantom_type()
        self.check_jmeter_type()
        self.check_bfg_type()

        self.log.info("Load Generators: %s" % self.generator_types)
        if "jmeter" in self.generator_types and len(self.generator_types) > 1:
            self.log.warning("Not only JMeter load generator active. "
                             "All active load generators: %s" \
                             % self.generator_types)

        # Генераторы при отображении в отчетах будут отсортированы по алфавиту.
        self.generator_types.sort()

        rps_report = self.rps_report.strip()
        if rps_report:
            self.rps = rps_report
        if not self.rps:
            self.rps = ' '  # иначе "поедет" таблица в jira

        return {'list': self.generator_types,
                'str': ' '.join(self.generator_types)}

    def get_http_codes_report(self, stage='post_process'):
        total = self.calc_value('net_info', stage)[0]
        http_codes_stat = self.calc_value('http_info', stage)[0]
        _report = "||Кол-во||%||код HTTP||Описание||\n"
        try:
            for code, code_total in http_codes_stat.items():
                _report += "|{count}|{percent}%|{code}|{code_text}|\n".format(
                                code=code, count=code_total,
                                percent="%.2f" \
                                    % (float(code_total)*100/total),
                                code_text=Codes.HTTP.get(int(code), 'N/A'))
            _report += "||{count}||{percent}%|| || ||\n".format(count=total,
                                                                percent=100)
        except Exception as exc:
            self.log.warning("Exception on http codes report: %s" % exc)
            return ''
        return _report

    def get_net_codes_report(self, stage='post_process'):
        (total, total_net_errors, net_codes_stat) = \
            self.calc_value('net_info', stage)
        _report = "||Кол-во||%||код NET||Описание||\n"
        try:
            for code, code_total in net_codes_stat.items():
                _report += "|{count}|{percent}%|{code}|{code_text}|\n".format(
                    code=code, count=code_total,
                    percent="%.2f" % (float(code_total)*100/total),
                                      code_text=Codes.NET.get(int(code),
                                                              'N/A'))
            _report += "||{count}||{percent}%|| || ||\n".format(count=total,
                                                                percent=100)
        except Exception as exc:
            self.log.warning("Exception on net codes report.")
        return _report

    def get_http_codes_aggreg_report(self, stage='post_process'):
        total = self.calc_value('net_info', stage)[0]
        (http_codes_stat, total_http_errors, http_codes_aggreg_stat) = \
            self.calc_value('http_info', stage)
        _report = "||Кол-во||%||коды HTTP||\n"
        try:
            for code, code_total in http_codes_aggreg_stat.items():
                _report += "|{count}|{percent}%|{code}xx|\n".format(
                    code=code, count=code_total,
                    percent="%.2f" % (float(code_total)*100/total))
            _report += "||{count}||{percent}%|| ||\n".format(count=total,
                                                             percent=100)
        except Exception as exc:
            self.log.warning("Exception on http codes "
                             "aggregation report: %s" % exc)
        return _report

    def get_edit_results_url(self, stage='post_process'):
        salts_plugin = self.salts_plugins.get('Salts')
        if not salts_plugin:
            return ''
        return salts_plugin.generate_edit_results_url()

    def get_peaks_report(self, stage='post_process', *args):
        if not self.peaks:
            if not self.core.config.config.has_section(JSON_SECTION):
                self.log.info("Timeouts Section [%s] is absent "
                                "in config." % JSON_SECTION)
                return ''

            timeout_options = self.core.config.config.options(JSON_SECTION)
            defaults = self.core.config.config.defaults()
            for option in defaults:
                timeout_options.remove(option)
            thresholds = {}
            for option in timeout_options:
                thresholds[option] = self.core.config.config.get(JSON_SECTION,
                                                                    option)
            self.log.info("Thresholds: %s" % thresholds)

            generator_type = self.calc_value('gen_type', 'prepare_test')
            generator_type = generator_type["str"]
            lt_log_file = None
            if generator_type == 'jmeter':
                jmeter = self.core.get_plugin_of_type(JMeterPlugin)
                lt_log_file = jmeter.jtl_file
                self.log.info("JMeter Log File: %s" % lt_log_file)
            elif generator_type == 'phantom':
                phantom = self.core.get_plugin_of_type(PhantomPlugin)
                lt_log_file = phantom.phantom.phout_file
                self.log.info("Phantom Log File: %s" % lt_log_file)

            if not lt_log_file:
                return ''
            self.peaks = PeaksCount(self.data['system_version'],
                                    generator_type,
                                    lt_log_file,
                                    thresholds)
        return self.peaks.get_report(args[0])


    def get_short_summary(self, stage='post_process'):
        keys = ['rps', 'q99', 'q90', 'q50', 'http_perc_str',
                'net_perc_str', 'duration']
        values = {}
        for k in keys:
            values[k] = self.calc_value(k, stage)
        _report = ''
        try:
            _report = "rps {rps}, 99% < {q99}ms, 90% < {q90}ms, " \
                      "50% < {q50}ms, http ошибки {http_perc_str}%, " \
                      "net ошибки {net_perc_str}%, " \
                      "длительность {duration}".format(**values)
        except Exception, exc:
            self.log.warning("Exception on short report: %s" % exc)
        return _report

    def get_versions_report(self, stage='post_process', *args):
        report_type = args[0]
        pack = args[1]
        packages = self.services
        if pack == 'packages':
            if report_type == 'jira':
                packages = self.email_packages
            else:
                packages = self.packages

        headers = [u"Имя пакета", u"Версия"]
        report = ''
        try:
            report = generate_salts_report(headers,
                                             packages_to_tuples(packages),
                                             report_type)
        except Exception, exc:
            self.log.warning("Exception on versions report: %s" % exc)

        return report

    def get_warnings(self, stage='post_process'):
        return '\n'.join(self.report_handler.get_messages())

    def get_rps_schedule(self, stage='start_test'):
        rps = self.calc_value('rps', stage)
        msg_parts = []
        for r in self.rps:
            msg_parts.append("%s - %s" % (r, self.rps[r]))
        return ", ".join(msg_parts)

    def get_planned_duration(self, stage='prepare_test'):
        duration = {}
        try:
            phantom = self.core.get_plugin_of_type(PhantomPlugin)
            s = 'phantom'
            duration[s] = 0
            for stream in phantom.phantom.streams:
                if stream.stepper_wrapper.duration > duration[s]:
                    duration[s] = stream.stepper_wrapper.duration
        except Exception, exc:
            self.log.warning("Phantom plugin hasn't been loaded: %s" % exc)
        try:
            jmeter = self.core.get_plugin_of_type(JMeterPlugin)
            s = 'jmeter'
            duration[s] = 0
            if jmeter.SECTION:
                duration[s] += \
                    int(self.core.get_option(jmeter.SECTION,
                                             'rampup', '0').strip())
                duration[s] += \
                    int(self.core.get_option(jmeter.SECTION,
                                             'testlen', '0').strip())
                duration[s] += \
                    int(self.core.get_option(jmeter.SECTION,
                                             'rampdown', '0').strip())
        except Exception, exc:
            self.log.warning("JMeter plugin hasn't been loaded: %s" % exc)

        d = {'seconds': 0}
        if duration:
            d['seconds'] = max(duration.values())
        d['timedelta'] = datetime.timedelta(seconds=d['seconds'])
        return d

    def save_metrics(self, stage):
        (total, total_net_errors, net_codes_stat) = \
            self.calc_value('net_info', stage)
        (http_codes_stat, total_http_errors, http_codes_aggreg_stat) = \
            self.calc_value('http_info', stage)
        duration = self.calc_value('duration', stage)
        try:
            results = {
                'overall': self.overall,
                'cumulative': self.cumulative,
                'cases': self.cases,
                'monitoring': self.mon_data,
                'total': total,
                'total_http_errors': total_http_errors,
                'total_net_errors': total_net_errors,
                'net_codes_stat': net_codes_stat,
                'http_codes_stat': http_codes_stat,
                'http_codes_aggreg_stat': http_codes_aggreg_stat,
                'duration': duration,
            }

            if self.save_metrics_dump:
                metrics_file_name = self.core.mkstemp(METRICS_FILE_SUFFIX,
                                                      METRICS_FILE_PREFIX)
                self.core.add_artifact_file(metrics_file_name)
                self.log.info("SALTSReport: metrics_file_name = %s" \
                              % metrics_file_name)
                with open(metrics_file_name, 'w') as metrics_file:
                    metrics_file.write(json.dumps(results))
        except Exception, exc:
            self.log.warning("Exception on metric file creating: %s" % exc)

    def pause_for_reading(self):
        if self.data['yandex_api_server']:
            return
        time.sleep(READING_TIME)

    def import_salts_plugin(self, title):
        try:
            module_name = 'yatank_%s' % title
            class_name = 'yatank_%sPlugin' % title
            plugin_module = importlib.import_module(module_name)
            plugin_class = getattr(plugin_module, class_name)
            return self.core.get_plugin_of_type(plugin_class)
        except (ImportError, KeyError), exc:
            self.log.info("Requested yatank-%s plugin is not available: %s. Module: %s" \
                            % (title, exc, module_name))

    def generate_session_id(self):
        if not self.session_id:
            if self.data['yandex_api_server']:
                self.session_id = self.core.tank_worker.session_id
                self.core.artifacts_dir = "."
            else:
                date_str = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S.")
                self.core.artifacts_dir = tempfile.mkdtemp("", date_str, self.core.artifacts_base_dir)
                self.session_id = os.path.basename(self.core.artifacts_dir)
            self.log.info("Artifacts Dir: %s" % os.path.abspath(self.core.artifacts_dir))
            self.data['artifacts_dir'] = os.path.abspath(self.core.artifacts_dir)

    def check_phantom_type(self):
        def remove_dups(seq):
            seen = set()
            seen_add = seen.add
            return [ x for x in seq if not (x in seen or seen_add(x))]

        if "phantom" in self.rps:
            return
        rps_line = ""
        try:
            phantom = self.core.get_plugin_of_type(PhantomPlugin)
            if phantom.SECTION:
                address_ar = []
                rps_ar = []
                instances_ar = []
                for section in [phantom.SECTION] + self.core.config.find_sections(phantom.SECTION + '-'):
                    _address_parts = []
                    _address_parts.append(self.core.get_option(section, 'address', ''))
                    _port = self.core.get_option(section, 'port', '')
                    if _port:
                        _address_parts.append(_port)
                    address_ar.append(":".join(_address_parts))
                    _value = self.core.get_option(section, 'rps_schedule', '')
                    if _value:
                        rps_ar.append('r:' + _value)
                    _value = self.core.get_option(section, 'instances_schedule', '')
                    if _value:
                        instances_ar.append('i:' + _value)
                self.data['target'] = "; ".join(remove_dups(address_ar))
                rps_line = "; ".join(rps_ar)
                instances = "; ".join(instances_ar)
                if instances:
                    if rps_line:
                        rps_line += '; ' + instances
                    else:
                        rps_line = instances
                self.generator_types.append("phantom")
        except KeyError:
            self.log.info("Phantom plugin disabled")
        if rps_line:
            self.rps["phantom"] = rps_line

    def check_jmeter_type(self):
        if "jmeter" in self.rps:
            return
        rps_line = ""
        try:
            port = ""
            jmeter = self.core.get_plugin_of_type(JMeterPlugin)
            if jmeter.SECTION:
                self.data['target'] = self.core.get_option(jmeter.SECTION, 'hostname', '').strip()
                if not self.data['target']:
                    self.data['target'] = self.core.get_option(jmeter.SECTION, 'host', '').strip()
                port = self.core.get_option(jmeter.SECTION, 'port', '')
                rps_line = self.core.get_option(jmeter.SECTION, 'rps', '')
                if not rps_line:
                    rps1 = self.core.get_option(jmeter.SECTION, 'rps1', '')
                    rps2 = self.core.get_option(jmeter.SECTION, 'rps2', '')
                    if rps1 != rps2:
                        rps_line = rps1 + ".." + rps2
                    else:
                        rps_line = rps2
            if port:
                self.data['target'] += ":" + str(port)
            self.generator_types.append("jmeter")
        except KeyError:
            self.log.info("JMeter plugin disabled")
        if rps_line:
            self.rps["jmeter"] = rps_line

    def check_bfg_type(self):
        if "bfg" in self.rps:
            return
        rps_line = ""
        try:
            port = ""
            bfg = self.core.get_plugin_of_type(bfgPlugin)
            rps_ar = []
            instances_ar = []
            if bfg.SECTION:
                gun_type = self.core.get_option(bfg.SECTION, 'gun_type', '').strip()
                if gun_type == 'sql':
                    gun_section = 'sql_gun'
                    self.data['target'] = self.core.get_option(gun_section, 'db', '').strip()
                    tmatched = re.match('postgresql://.+?:.+?@(.+)', self.data['target'])
                    if tmatched:
                        self.data['target'] = tmatched.group(1)
                port = ''
                #port = self.core.get_option(bfg.SECTION, 'port', '')
                _value = self.core.get_option(bfg.SECTION, 'rps_schedule', '')
                if _value:
                    rps_ar.append('r:' + _value)
                _value = self.core.get_option(bfg.SECTION, 'instances_schedule', '')
                if _value:
                    instances_ar.append('i:' + _value)
                rps_line = "; ".join(rps_ar)
                instances = "; ".join(instances_ar)
                if instances:
                    if rps_line:
                        rps_line += '; ' + instances
                    else:
                        rps_line = instances
            if port:
                self.data['target'] += ":" + str(port)
            self.generator_types.append("bfg")
        except KeyError:
            self.log.info("BFG plugin disabled")
        if rps_line:
            self.rps["bfg"] = rps_line


    def get_versions(self):
        targets = self.data['target'].split("; ") or [self.data['target']]
        for target_url in targets:
            (version, message, log_level) = target_version(target_url,
                                                           self.version_url_path)
            self.services[target_url] = version
            if log_level == HAS_WARNING or version == UNKNOWN_VERSION:
                self.log.warning(message)
                self.pause_for_reading()
            else:
                self.log.info(message)
            if version != UNKNOWN_VERSION:
                (sips_ok, message) = sips_check(self.sips_package,
                                                 self.sips_repo,
                                                 version)
                if sips_ok:
                    self.log.info(message)
                else:
                    self.log.warning(message)
                    self.pause_for_reading()
        versions_value = ""
        unk_version = True
        for target_url in targets:
            if versions_value:
                versions_value += ";"
            ver = self.services[target_url]
            unk_version = unk_version and ver == UNKNOWN_VERSION
            versions_value += ver

        if (not unk_version) or (unk_version and len(self.services) > 1):
            if self.data.get('system_version'):
                self.data['system_version'] = "%s, %s" \
                    % (self.data['system_version'], versions_value)
            else:
                self.data['system_version'] = versions_value
        else:
            if self.data.get('system_version'):
                self.data['system_version'] = "%s, %s" \
                    % (self.data['system_version'], UNKNOWN_VERSION)
            else:
                self.data['system_version'] = UNKNOWN_VERSION
        self.log.info("Full version is %s" % self.data['system_version'])

    def aggregate_second(self, data):
        """
        @data: SecondAggregateData
        """
        ts = int(time.mktime(data.time.timetuple()))

        def add_aggregated_second(data_item, storage):
            data_dict = data_item.__dict__
            avg = storage['avg']
            for key in ["avg_connect_time", "avg_send_time", "avg_latency", "avg_receive_time"]:
                avg[key].append((ts, data_dict.get(key, None)))
            quantiles = storage['quantiles']
            for key, value in data_item.quantiles.iteritems():
                quantiles[key].append((ts, value))
            storage['threads']['active_threads'].append((ts, data_item.active_threads))
            storage['rps']['RPS'].append((ts, data_item.RPS))
            http_codes = storage['http_codes']
            for key, value in data_item.http_codes.iteritems():
                http_codes[key].append((ts, value))
            net_codes = storage['net_codes']
            for key, value in data_item.net_codes.iteritems():
                net_codes[key].append((ts, value))

        def add_aggregated_second_cumulative(data_item, storage):
            data_dict = data_item.__dict__
            avg = storage['avg']
            for key in ["avg_connect_time", "avg_send_time", "avg_latency", "avg_receive_time"]:
                avg[key].append((ts, data_dict.get(key, None)))
            quantiles = storage['quantiles']
            for key, value in data_item.quantiles.iteritems():
                quantiles[key].append((ts, value))

        add_aggregated_second(data.overall, self.overall)
        add_aggregated_second_cumulative(data.cumulative, self.cumulative)
        for case, case_data in data.cases.iteritems():
            add_aggregated_second(case_data, self.cases[case])
        self.rps_values.append(data.overall.RPS)

    def monitoring_data(self, data_string):
        self.log.debug("Mon report data: %s", data_string)
        for line in data_string.splitlines():
            if not line.strip():
                continue

            def append_data(host, ts, data):
                if host not in self.mon_data:
                    self.mon_data[host] = {}
                host_data = self.mon_data[host]
                for key, value in data.iteritems():
                    try:
                        value = float(value)
                        if '_' in key:
                            group, key = key.split('_', 1)
                        else:
                            group = key
                        if group not in host_data:
                            host_data[group] = {}
                        group_data = host_data[group]
                        if key not in group_data:
                            group_data[key] = []
                        group_data[key].append((int(ts), value))
                    except ValueError:
                        pass

            host1, data1, _, ts1 = self.decoder.decode_line(line)
            append_data(host1, ts1, data1)

    def get_available_options(self):
        return ['test_name', 'test_group', 'mail_from', 'mail_to',
                'quantiles_fml', 'ltm_url', 'save_metrics_dump',
                'http_error_codes', 'version', 'ticket_url',
                'grafana_dashboard_url', 'force_run',
                'console_default_api_user', 'console_default_api_key',
                'rps_report', 'email_packages', 'results_url_query',
                'version_url_path', 'sips_package', 'sips_repo',
                'base_ticket_url', 'ticket_id']

    def configure(self):
        from ConfigParser import NoOptionError
        """Read configuration"""
        self.data['test_name'] = self.get_option('test_name', "#TestName#")
        self.data['test_group'] = self.get_option('test_group', "#TestGroup#")
        self.data['scenario_path'] = self.get_option('scenario_path',
                                                     'unknown')
        self.data['ltm_url'] = self.get_option("ltm_url", "#LTM_URL#")
        # кол-во значений для find_max_last
        self.data['quantiles_fml'] = int(self.get_option('quantiles_fml', 7))
        self.mail_from = self.get_option("mail_from", self.DEFAULT_MAIL_FROM)
        self.mail_to = self.get_option("mail_to", "")
        self.save_metrics_dump = bool(self.get_option("save_metrics_dump", True))
        self.http_error_codes = self.get_option("http_error_codes", r'5[0-9][0-9]')
        self.data['system_version'] = self.get_option("version", "")
        self.ticket_url = self.get_option('ticket_url', '')
        try:
            self.ticket_id = self.get_option('ticket_id')
        except NoOptionError:
            self.log.warning("Option 'ticket_id' is absent.")
        self.base_ticket_url = self.get_option('base_ticket_url', '')
        self.grafana_dashboard_url = self.get_option("grafana_dashboard_url",
                                                     self.DEFAULT_GRAFANA_DASHBOARD_URL)
        self.data['console_default_api_key'] = \
            self.get_option('console_default_api_key', '')
        self.data['console_default_api_user'] = \
            self.get_option('console_default_api_user', '')
        if not self.data['yandex_api_server']:
            self.data['force_run'] = self.get_option('force_run', 0)
        self.version_url_path = self.get_option('version_url_path',
                                                self.DEFAULT_VERSION_URL_PATH)
        try:
            aggregator = self.core.get_plugin_of_type(AggregatorPlugin)
            aggregator.add_result_listener(self)
        except Exception, exc:
            self.log.warning("No aggregator module, "
                             "no valid report will be available: %s" % exc)

        try:
            mon = self.core.get_plugin_of_type(MonitoringPlugin)
            if mon.monitoring:
                mon.monitoring.add_listener(self)
        except Exception, exc:
            self.log.warning("No monitoring module, "
                             "monitoring report disabled: %s" % exc)
        self.rps_report = self.core.get_option(self.SECTION, 'rps_report', '')
        self.sips_package = self.get_option("sips_package", "")
        self.sips_repo = self.get_option("sips_repo", "")

        for name in self.salts_plugins:
            self.salts_plugins[name] = self.import_salts_plugin(name)

        online = self.salts_plugins.get('SputnikOnline')
        if online:
            self.data['web_console'] = {'hostname': full_hostname(),
                                        'port': online.port}

        if not self.salts_plugins['Salts']:
            self.log.info("Salts plugin is disabled. "
                          "The interaction with DB is impossible.")
            default_user = self.data.get('console_default_api_user')
            if self.data['yandex_api_server'] and default_user:
                self.user_name = default_user
            else:
                self.user_name = getpass.getuser()
            return

    def prepare_test(self):
        self.packages = provide_package_versions()
        self.email_packages = {}
        for pack in re.split("[,; ]+",
                             self.get_option('email_packages',
                                             self.EMAIL_PACKAGES)):
            if pack in self.packages:
                self.email_packages[pack] = self.packages[pack]
                self.log.info("Version of %s package is %s" \
                              % (pack, self.email_packages[pack]))
            else:
                self.log.warning("Version of %s package not available." \
                                 % pack)
                self.pause_for_reading()
        # при получении gen_type получаем также target,
        # который используется в последующем вызове get_versions
        self.calc_value('gen_type', 'prepare_test')
        self.get_versions()

    def start_test(self):
        pass

    def end_test(self, retcode):
        self.data['retcode'] = retcode
        if retcode:
            self.data['hipchat_message_color'] = 'red'
        else:
            self.data['hipchat_message_color'] = 'green'
        if not self.salts_plugins['Salts']:
            self.log.info("Salts plugin is disabled. "
                          "The interaction with DB is impossible.")
            return retcode

        return retcode

    def post_process(self, retcode):
        self.save_metrics('post_process')
        return retcode
