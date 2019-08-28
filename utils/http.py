# (C) Datadog, Inc. 2010-2016
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)

import requests


DEFAULT_TIMEOUT = 10
# If we're running as root and this user exists, we'll drop privileges.
USER = "cwiz-user"


class RevertibleLowPrivilegeUser(object):
    def __init__(self, low_privelege_user, logger):
        self.low_privilege_user = low_privelege_user
        self.logger = logger

    def __enter__(self):
        pass
#        if os.geteuid() != 0:
#            return
#        try:
#            ent = pwd.getpwnam(self.low_privilege_user)
#        except KeyError:
#            return

#        self.logger.info("set to lower-privilege user %s", self.low_privilege_user)
#        os.setegid(ent.pw_gid)
#        os.seteuid(ent.pw_uid)

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
#       self.logger.info("revert. set current euser %s back to %s", os.geteuid(), os.getuid())
#       os.seteuid(os.getuid())


def lower_privileges(logger, user=USER):
    return RevertibleLowPrivilegeUser(user, logger)


def retrieve_json(url, timeout=DEFAULT_TIMEOUT, verify=True):
    r = requests.get(url, timeout=timeout, verify=verify)
    r.raise_for_status()
    return r.json()

# Get expvar stats
def get_expvar_stats(key, host="localhost", port=5000):
    try:
        json = retrieve_json("http://{host}:{port}/debug/vars".format(host=host, port=port))
    except requests.exceptions.RequestException as e:
        raise e

    if key:
        return json.get(key)

    return json


def alertd_post_sender(url, data, payload={}, token=None, skip_ssl_validation=True):
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

    cookies = dict(_token=token)
    # print '%s%s?token=%s' % (metrics_server, url, token)
    if len(payload):
        payload["token"] = token
    else:
        payload = {'token': token}
    req = requests.post(url, params=payload, json=data, headers=headers, cookies=cookies,
                        timeout=20, verify=(not skip_ssl_validation))
    return req
