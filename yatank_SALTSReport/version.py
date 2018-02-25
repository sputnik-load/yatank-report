# -*- coding: utf-8 -*-

import urlparse
import validators
import xmltodict
import requests
import socket
import re
from datetime import datetime


TIMEOUT=3
UNKNOWN_VERSION="-"
HAS_WARNING=True


def xml_value(content, key):
    if key in content:
        yield content[key]
    for k in content:
        if isinstance(content[k], dict):
            for j in xml_value(content[k], key):
                yield j


def find_in_http(r):
    if "x-version" in r.headers:
        version = r.headers["x-version"]
        if "x-revision" in r.headers:
            revision = r.headers["x-revision"]
            if revision:
                version = "%s r%s" % (version, revision)
        return (version,
                "Service version on %s is %s" % (r.url, version))
    else:
        return (UNKNOWN_VERSION,
                "Service version not available on %s" % r.url)


def find_in_xml(r):
    try:
        content = xmltodict.parse(r.text)
    except xmltodict.ParsingInterrupted:
        return (UNKNOWN_VERSION,
                "Response on %s contains invalid xml content." % r.url)
    versions = list(xml_value(content, "version")) + list(xml_value(content, "@version"))
    if not versions:
        return (UNKNOWN_VERSION,
                "Response on %s: no version info in xml content." % r.url)
    ver = versions[0]
    revs = list(xml_value(content, "revision")) + list(xml_value(content, "@revision"))
    if revs and revs[0]:
        ver = "%s r%s" % (ver, revs[0])
    return (ver, "Version url: %s. Service version is %s" % (r.url, ver))


def find_in_json(r):
    return (UNKNOWN_VERSION, "JSON Content Type isn't supported yet.")


def _get_version(url, path):
    url += path
    r = requests.get(url, timeout=TIMEOUT)
    if r.status_code != 200:
        return (UNKNOWN_VERSION,
                "Response on %s contains %d Status Code." % (url,
                                                             r.status_code))
    if "Content-Type" not in r.headers:
        return (UNKNOWN_VERSION,
                "Response on %s doen't contain 'Content-Type' field." % url)

    content_type = r.headers["Content-Type"]
    if re.search("html", content_type):
        return find_in_http(r)
    elif re.search("xml", content_type):
        return find_in_xml(r)
    elif re.search("json", content_type):
        return find_in_json(r)
    else:
        return (UNKNOWN_VERSION,
                "Content type '%s' on request %s isn't supported." % (content_type, url))


def target_version(url, path):
    try:
        warn_message = ""
        pr = urlparse.urlparse(url)
        if not pr.scheme:
            url = "http://%s" % url
            pr = urlparse.urlparse(url)
            name = pr.netloc.split(":")[0]
            ip = socket.gethostbyname(name)
            try:
                host_info = socket.gethostbyaddr(ip)
            except socket.error as e:
                warn_message = "Exception: %s. Url: %s. Path: %s." % (e, url, path)
            else:
                url = url.replace(name, host_info[0])
        if not validators.url(url):
            if warn_message:
                warn_message += " "
            warn_message += "Invalid version url is generated: %s." % url

        (ver, msg) = _get_version(url, path)
        if warn_message:
            msg = "%s %s" % (msg, warn_message)
            return (ver, msg, HAS_WARNING)
        else:
            return (ver, msg, not HAS_WARNING)

    except Exception as e:
        return (UNKNOWN_VERSION,
                "Exception: %s. Url: %s. Path: %s." % (e, url, path),
                HAS_WARNING)


def sips_check(package, repo, version):
    def extr_ver_rev(tup):
        ver = ""
        rev = ""
        if len(tup) > 0:
            ver = tup[0]
        if len(tup) > 1:
            rev = tup[1]
        return (ver, rev)

    if not repo or not package:
        return (False, "Sips check cannot executed. Repo or package are empty.")
    r = requests.get(repo, timeout=TIMEOUT)
    if r.status_code != 200:
        return (False, "Sips repo on %s isn't available." % repo)

    regex = re.compile("<a.*%s.*</a> *(\d\d-\w\w\w-\d\d\d\d \d\d:\d\d) *\d+" % package)

    vers = {}
    for match in regex.finditer(r.text):
        gr = match.groups()
        (ver, rev) = extr_ver_rev(gr[:-1])
        vers[datetime.strptime(gr[-1], "%d-%b-%Y %H:%M")] = (ver, rev)
    max_datetime = max(vers.keys())
    (actual_ver, actual_rev) = vers[max_datetime]

    parts = version.split()
    if parts[0] != actual_ver:
        return (False,
                "Service version: %s. Sips version: %s. Not matched." % (parts[0],
                                                                         actual_ver))
    if len(parts) > 1:
        if not actual_rev:
            return (False,
                    "Sips version: %s. Revision is empty." % actual_ver)

        if parts[1] != actual_rev:
            return (False,
                    "Versions are matched but revisions aren't matched. \
Service revision: %s. Sips revision: %s." % (parts[1], actual_rev))
    else:
        if actual_rev:
            return (False,
                    "Versions are matched but revision %s isn't expected." % actual_rev)

    ver_message = ""
    if actual_ver:
        ver_message = "Version is %s." % actual_ver
        if actual_rev:
            ver_message = "%s Revision is %s" % (ver_message, actual_rev)

    return (True, "Sips check passed. %s" % ver_message)
