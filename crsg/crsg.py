# Script written by Aleksandr Alekseev
# Original source https://gitlab.cern.ch/atlas-adc-monitoring/grafana-api/-/blob/master/Examples/CRSG%20report/Scripts/crsg.py
#
# Before using, add the grafana access token below (where is says FIX ME)
# The token can be found at the above location

import json
import pytz
import pandas as pd
import sys, getopt, os

from datetime import datetime, timedelta
from json import loads
from requests import post


class CRSG:
    def __init__(self):
        # Fill in the missing token by checking original source file (linked above)
        self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": "FIX ME !!!"
        }

    def getData(self, date_from, date_to, rtype):

        base_url = "https://monit-grafana.cern.ch"
        datasource_url = "api/datasources/proxy/9559/_msearch?max_concurrent_shard_requests=256"

        date_from_obj = datetime.strptime(date_from, '%Y-%m-%d')
        date_to_obj = datetime.strptime(date_to, '%Y-%m-%d')

        date_from_t = date_from_obj.replace(tzinfo=pytz.utc, hour=00, minute=00, second=00, microsecond=00)
        date_to_t = date_to_obj.replace(tzinfo=pytz.utc, hour=23, minute=59, second=59)

        date_from_ms = str(int(date_from_t.timestamp() * 1000))
        date_to_ms = str(int(date_to_t.timestamp() * 1000))

        time_range = """"gte":{0},"lte":{1}""".format(date_from_ms, date_to_ms)

        request = """{"search_type":"query_then_fetch","ignore_unavailable":true,"index":["monit_prod_atlasjm_agg_completed*"]}\n{"size":1000,"query":{"bool":{"filter":[{"range":{"metadata.timestamp":{""" + time_range + ""","format":"epoch_millis"}}},{"query_string":{"analyze_wildcard":true,"query":"*"}}]}},"aggs":{"computingsite":{"terms":{"field":"data.computingsite","size":1000,"order":{"_key":"desc"},"min_doc_count":1},"aggs":{"site":{"terms":{"field":"data.dst_experiment_site","size":1000,"order":{"_key":"desc"},"min_doc_count":1},"aggs":{"federation":{"terms":{"field":"data.dst_federation","size":1000,"order":{"_key":"desc"},"min_doc_count":1},"aggs":{"tier":{"terms":{"field":"data.dst_tier","size":1000,"order":{"_key":"desc"},"min_doc_count":1},"aggs":{"resourcetype":{"terms":{"field":"data.resource_type","size":1000,"order":{"_key":"desc"},"min_doc_count":1},"aggs":{"sum_hs06sec":{"sum":{"field":"data.sum_hs06sec"}},"sum_walltime":{"sum":{"field":"data.sum_walltime"}},"sum_cpuconsumptiontime":{"sum":{"field":"data.sum_cpuconsumptiontime"}}}}}}}}}}}}}}\n"""

        request_url = "%s/%s" % (base_url, datasource_url)

        r = post(request_url, headers=self.headers, data=request)

        computingsites_dict = {}

        if r.ok:
            computingsites = loads(r.text)['responses'][0]['aggregations']['computingsite']['buckets']
            for computingsite in computingsites:
                computingsite_name = computingsite['key']
                for site in computingsite['site']['buckets']:
                    site_name = site['key']
                    for federation in site['federation']['buckets']:
                        federation_name = federation['key']
                        for tier in federation['tier']['buckets']:
                            tier_name = tier['key']
                            for resourcetype in tier['resourcetype']['buckets']:
                                resourcetype_name = resourcetype['key']
                                if rtype and rtype != resourcetype_name:
                                    continue
                                sum_walltime = \
                                    resourcetype['sum_walltime']['value']
                                sum_cpuconsumptiontime = \
                                    resourcetype['sum_cpuconsumptiontime']['value']
                                sum_hs06sec = \
                                    resourcetype['sum_hs06sec']['value']
                                computingsites_dict.setdefault(computingsite_name, []).append(
                                    {'computingsite': computingsite_name,
                                     'site': site_name,
                                     'federation': federation_name,
                                     'tier': tier_name,
                                     'resourcetype': resourcetype_name,
                                     'sum_walltime': sum_walltime,
                                     'sum_cpuconsumptiontime': sum_cpuconsumptiontime,
                                     'sum_hs06sec': sum_hs06sec
                                     })

        computingsites_list = []

        for computingsite, computingsites in computingsites_dict.items():
            sum_walltime = 0
            sum_hs06sec = 0
            sum_cpuconsumptiontime = 0

            for info in computingsites:
                if info['site'] == 'UNKNOWN' and info['federation'] == 'UNKNOWN' and info[
                    'resourcetype'] == 'UNKNOWN' and info['tier'] == -1:
                    sum_walltime += info['sum_walltime']
                    sum_hs06sec += info['sum_hs06sec']
                    sum_cpuconsumptiontime += info['sum_cpuconsumptiontime']
                else:
                    site = info['site']
                    federation = info['federation']
                    tier = info['tier']
                    resourcetype = info['resourcetype']
                    sum_walltime += info['sum_walltime']
                    sum_hs06sec += info['sum_hs06sec']
                    sum_cpuconsumptiontime += info['sum_cpuconsumptiontime']
                    try:
                        core_power = sum_hs06sec / sum_walltime
                    except:
                        core_power = 0
                    try:
                        ratio = sum_cpuconsumptiontime / sum_walltime * 100
                    except:
                        ratio = 0
                    wc_in_hours = sum_walltime / 3600

                    computingsites_list.append({
                        'computingsite': computingsite,
                        'site': site,
                        'federation': federation,
                        'tier': tier,
                        'resourcetype': resourcetype,
                        'sum_hs06sec': sum_hs06sec,
                        'sum_walltime': sum_walltime,
                        'sum_cpuconsumptiontime': sum_cpuconsumptiontime,
                        'core_power': round(core_power, 2),
                        'ratio': round(ratio, 2),
                        'wc_in_hours': round(wc_in_hours, 2)
                    })
                    sum_walltime = 0
                    sum_hs06sec = 0
                    sum_cpuconsumptiontime = 0

        sites_dict = {}
        federations_dict = {}
        tiers_dict = {}

        sites_list = []
        federations_list = []
        tiers_list = []
        # Tiers, Federations, Sites
        for computingsite in computingsites_list:

            site_name = computingsite['site']
            federation_name = computingsite['federation']
            tier_name = computingsite['tier']

            if site_name not in sites_dict:
                sites_dict[site_name] = {
                    'sum_walltime': computingsite['sum_walltime'],
                    'sum_cpuconsumptiontime': computingsite['sum_cpuconsumptiontime'],
                    'sum_hs06sec': computingsite['sum_hs06sec']
                }
            else:
                sites_dict[site_name]['sum_walltime'] += computingsite['sum_walltime']
                sites_dict[site_name]['sum_cpuconsumptiontime'] += computingsite['sum_cpuconsumptiontime']
                sites_dict[site_name]['sum_hs06sec'] += computingsite['sum_hs06sec']

            if federation_name not in federations_dict:
                federations_dict[federation_name] = {
                    'sum_walltime': computingsite['sum_walltime'],
                    'sum_cpuconsumptiontime': computingsite['sum_cpuconsumptiontime'],
                    'sum_hs06sec': computingsite['sum_hs06sec']
                }
            else:
                federations_dict[federation_name]['sum_walltime'] += computingsite['sum_walltime']
                federations_dict[federation_name]['sum_cpuconsumptiontime'] += computingsite['sum_cpuconsumptiontime']
                federations_dict[federation_name]['sum_hs06sec'] += computingsite['sum_hs06sec']

            if site_name == 'UNKNOWN':
                print('Site is UNKNOWN for {0}'.format(computingsite['computingsite']))
            if federation_name == 'UNKNOWN':
                print('Federation is UNKNOWN for {0}'.format(computingsite['computingsite']))
            if tier_name == -1:
                print('TIER = -1 for {0}'.format(computingsite['computingsite']))

            if tier_name not in tiers_dict:
                tiers_dict[tier_name] = {
                    'sum_walltime': computingsite['sum_walltime'],
                    'sum_cpuconsumptiontime': computingsite['sum_cpuconsumptiontime'],
                    'sum_hs06sec': computingsite['sum_hs06sec']
                }
            else:
                tiers_dict[tier_name]['sum_walltime'] += computingsite['sum_walltime']
                tiers_dict[tier_name]['sum_cpuconsumptiontime'] += computingsite['sum_cpuconsumptiontime']
                tiers_dict[tier_name]['sum_hs06sec'] += computingsite['sum_hs06sec']
        for site, info in sites_dict.items():
            try:
                core_power = info['sum_hs06sec'] / info['sum_walltime']
            except:
                core_power = 0
            try:
                ratio = info['sum_cpuconsumptiontime'] / info['sum_walltime'] * 100
            except:
                ratio = 0
            wc_in_hours = info['sum_walltime'] / 3600

            sites_list.append(
                {
                    'site': site,
                    'walltime': int(info['sum_walltime']),
                    'cpuconsumptiontime': int(info['sum_cpuconsumptiontime']),
                    'hs06sec': int(info['sum_hs06sec']),
                    'core_power': round(core_power, 2),
                    'ratio': round(ratio, 2),
                    'wc_in_hours': round(wc_in_hours, 2)
                }
            )

        for federation, info in federations_dict.items():
            try:
                core_power = info['sum_hs06sec'] / info['sum_walltime']
            except:
                core_power = 0
            try:
                ratio = info['sum_cpuconsumptiontime'] / info['sum_walltime'] * 100
            except:
                ratio = 0
            wc_in_hours = info['sum_walltime'] / 3600

            federations_list.append(
                {
                    'federation': federation,
                    'walltime': int(info['sum_walltime']),
                    'cpuconsumptiontime': int(info['sum_cpuconsumptiontime']),
                    'hs06sec': int(info['sum_hs06sec']),
                    'core_power': round(core_power, 2),
                    'ratio': round(ratio, 2),
                    'wc_in_hours': round(wc_in_hours, 2)
                }
            )

        for tier, info in tiers_dict.items():
            try:
                core_power = info['sum_hs06sec'] / info['sum_walltime']
            except:
                core_power = 0
            try:
                ratio = info['sum_cpuconsumptiontime'] / info['sum_walltime'] * 100
            except:
                ratio = 0
            wc_in_hours = info['sum_walltime'] / 3600

            tiers_list.append(
                {
                    'tier': tier,
                    'walltime': int(info['sum_walltime']),
                    'cpuconsumptiontime': int(info['sum_cpuconsumptiontime']),
                    'hs06sec': int(info['sum_hs06sec']),
                    'core_power': round(core_power, 2),
                    'ratio': round(ratio, 2),
                    'wc_in_hours': round(wc_in_hours, 2)
                }
            )

        reports = {
            'computingsites': computingsites_list,
            'sites': sites_list,
            'federations': federations_list,
            'tiers': tiers_list
        }
        return reports

    def writerCSVeport(self, list, path, filename):
        df = pd.DataFrame(list)
        df.to_csv('{0}/{1}.csv'.format(path, filename))


def main(argv):
    date_from = ''
    date_to = ''
    output = ''
    rtype = None

    try:
        opts, args = getopt.getopt(argv, "hf:t:o:r:", ["from=", "to=", "out=", "rtype="])
    except getopt.GetoptError:
        print('crsg.py -from -to -output')
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("--from", "-f"):
            date_from = str(arg)
        elif opt in ("--to", "-t"):
            date_to = str(arg)
        elif opt in ("--out", "-o"):
            output = str(arg)
        elif opt in ("--rtype", "-r"):
            rtype = str(arg)

    if (date_from != '' and date_to != '' and output != ''):
        r = CRSG()

        reports = r.getData(date_from, date_to, rtype)
        os.makedirs(output, exist_ok=True)
        for report in reports:
            r.writerCSVeport(reports[report], output, report)
        print("completed")
    else:
        print('crsg.py --from --to --output')
        sys.exit(2)


if __name__ == "__main__":
    main(sys.argv[1:])
