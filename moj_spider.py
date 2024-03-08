# -*- coding: utf-8 -*-
import json
import logging
import traceback
import scrapy.spiders
from datetime import datetime, timedelta, date
from calendar import monthrange

logger = logging.getLogger(__name__)


class MojSpider(scrapy.Spider):
    name = 'moj'
    lmt_enabled = False
    proxymesh_enabled = True

    custom_settings = {
        'DOWNLOAD_DELAY': 0.05,
        'RETRY_HTTP_CODES': [400, 429, 403, 408, 500, 502, 503, 504, 522, 523],
        'S3_MI7_PATH_ACTIVE_ADS': 'competition/mi7/custom_crawlers/moj_gov_sa/'

    }

    def __init__(self, country='KSA', mode='serp_only', proxy='proxymesh'):
        self.args = locals()
        self.country = country
        self.delta_crawl = True
        self.mode = mode
        self.headers = dict()
        self.currency = 'SAR'
        self.site_id = 212
        self.region = 'saudi_arabia'
        self.site = 'moj.gov.sa'
        self.timezone = 'PKT'

        if proxy == 'lmt':
            self.lmt_enabled = False
        else:
            self.proxymesh_enabled = True

        # self.base_url = 'https://www.moj.gov.sa/'
        self.report_url = "https://wabi-west-europe-d-primary-api.analysis.windows.net/public/reports/querydata?synchronous=true"

        self.headers = {
            'X-PowerBI-ResourceKey': '4b99c877-0115-4e0a-b12f-72212fa3833c',
            'Content-Type': 'application/json'
        }
        self.query = {
            "version": "1.0.0",
            "queries": [
                {
                    "Query": {
                        "Commands": [
                            {
                                "SemanticQueryDataShapeCommand": {
                                    "Query": {
                                        "Version": 2,
                                        "From": [
                                            {
                                                "Name": "n",
                                                "Entity": "TransactionSale",
                                                "Type": 0
                                            }
                                        ],
                                        "Select": [
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {
                                                            "Source": "n"
                                                        }
                                                    },
                                                    "Property": "المنطقة"
                                                },
                                                "Name": "NotarizationWork.المنطقة"
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {
                                                            "Source": "n"
                                                        }
                                                    },
                                                    "Property": "المدينة"
                                                },
                                                "Name": "NotarizationWork.المدينة"
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {
                                                            "Source": "n"
                                                        }
                                                    },
                                                    "Property": "الحي"
                                                },
                                                "Name": "TransactionSale.الحي"
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {
                                                            "Source": "n"
                                                        }
                                                    },
                                                    "Property": "الرقم المرجعي للصفقة"
                                                },
                                                "Name": "CountNonNull(TransactionSale.الرقم المرجعي للصفقة)"
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {
                                                            "Source": "n"
                                                        }
                                                    },
                                                    "Property": "تاريخ الصفقة هجري"
                                                },
                                                "Name": "TransactionSale.HDate"
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {
                                                            "Source": "n"
                                                        }
                                                    },
                                                    "Property": "تاريخ الصفقة ميلادي"
                                                },
                                                "Name": "TransactionSale.تاريخ الصفقة ميلادي"
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {
                                                            "Source": "n"
                                                        }
                                                    },
                                                    "Property": "تصنيف العقار"
                                                },
                                                "Name": "TransactionSale.تصنيف العقار"
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {
                                                            "Source": "n"
                                                        }
                                                    },
                                                    "Property": "السعر"
                                                },
                                                "Name": "Sum(TransactionSale.السعر)"
                                            },
                                            {
                                                "Column": {
                                                    "Expression": {
                                                        "SourceRef": {
                                                            "Source": "n"
                                                        }
                                                    },
                                                    "Property": "المساحة"
                                                },
                                                "Name": "Sum(TransactionSale.المساحة)"
                                            },
                                            {
                                                "Measure": {
                                                    "Expression": {
                                                        "SourceRef": {
                                                            "Source": "n"
                                                        }
                                                    },
                                                    "Property": "عدد العقارات"
                                                },
                                                "Name": "TransactionSale.عدد العقارات"
                                            }
                                        ],
                                        "OrderBy": [
                                            {
                                                "Direction": 2,
                                                "Expression": {
                                                    "Column": {
                                                        "Expression": {
                                                            "SourceRef": {
                                                                "Source": "n"
                                                            }
                                                        },
                                                        "Property": "تاريخ الصفقة ميلادي"
                                                    }
                                                }
                                            },
                                            {
                                                "Direction": 2,
                                                "Expression": {
                                                    "Column": {
                                                        "Expression": {
                                                            "SourceRef": {
                                                                "Source": "n"
                                                            }
                                                        },
                                                        "Property": "السعر"
                                                    }
                                                }
                                            }
                                        ]
                                    },
                                    "Binding": {
                                        "Primary": {
                                            "Groupings": [
                                                {
                                                    "Projections": [
                                                        0,
                                                        1,
                                                        2,
                                                        3,
                                                        4,
                                                        5,
                                                        6,
                                                        7,
                                                        8,
                                                        9
                                                    ]
                                                }
                                            ]
                                        },
                                        "DataReduction": {
                                            "DataVolume": 15,
                                            "Primary": {
                                                "Window": {
                                                    "Count": 500
                                                }
                                            }
                                        },
                                        "Version": 2
                                    },
                                    "ExecutionMetricsKind": 1
                                }
                            }
                        ]
                    },
                    "QueryId": ""
                }
            ],
            "cancelQueries": [],
            "modelId": 2121030
        }

    def start_requests(self):
        retry_times = 0
        try:
            yield scrapy.Request(method='POST', url=self.report_url, callback=self.parse, headers=self.headers,
                                 body=json.dumps(self.query), meta={'retry_times': retry_times})

        except Exception as e:
            logger.error(f'start_requests  \n :{traceback.format_exc()}')

    def parse(self, response):
        retry_times = 0
        if response.status in self.custom_settings.get('RETRY_HTTP_CODES'):
            yield scrapy.Request(response.url, headers=self.headers, body=json.dumps(self.query), meta=response.meta,
                                 callback=self.parse)
            return
        try:

            data = response.json()
            if not data.get('results'):
                if response.meta['retry_times'] <= 50:
                    response.meta['retry_times'] += 1
                    logger.info(
                        f"Got Captcha At Parse On {response.url} - Retry Time {response.meta['retry_times']}/50")
                    yield scrapy.Request(response.url, headers=self.headers, body=json.dumps(self.query),
                                         meta=response.meta,
                                         callback=self.parse)
                    return
                else:
                    logger.info(
                        f"******* MAX RETRY ON CAPTCHA - Parse On {response.url} - Retry Time {response.meta['retry_times']}/50")


            else:
                # first_c = content['results'][0]['result']['data']['dsr']['DS'][0]['PH'][0]['DM0'][0]['C']
                # value_dicts = content['results'][0]['result']['data']['dsr']['DS'][0]['ValueDicts']
                first_c = \
                data.get('results', [])[0].get('result', {}).get('data', {}).get('dsr').get('DS')[0].get('PH')[0].get(
                    'DM0')[0].get('C', None)
                value_dicts = data.get('results', [])[0].get('result', {}).get('data', {}).get('dsr').get('DS')[0].get(
                    'ValueDicts', {})
                first_column = value_dicts['D0']
                second_column = value_dicts['D1']
                third_column = value_dicts['D2']
                fifth_islam_date_column = value_dicts['D3']
                sixth_eng_date_column = value_dicts['D4']
                seventh_column = value_dicts['D5']
                columns = [first_column, second_column, third_column, '', fifth_islam_date_column,
                           sixth_eng_date_column, seventh_column]
                # all_rows = content['results'][0]['result']['data']['dsr']['DS'][0]['PH'][0]['DM0']
                all_rows = \
                data.get('results', [])[0].get('result', {}).get('data', {}).get('dsr').get('DS')[0].get('PH')[0].get(
                    'DM0', {})
                firstrow_pop = all_rows.pop(0)
                firstdict = {'C': first_c}
                all_rows.insert(0, firstdict)
                last_month = date.today().replace(day=1) - timedelta(days=1)
                last_year = last_month.year
                last_month_number = last_month.month
                last_month_days = monthrange(last_year, last_month_number)[1]
                second_last_month = date.today() - timedelta(days=last_month_days + 1)
                third_last_month = date.today().replace(day=1, month=second_last_month.month,
                                                        year=second_last_month.year) - timedelta(days=1)
                second_last_month = second_last_month.strftime('%Y/%m')
                third_last_month = third_last_month.strftime('%Y/%m')

                for row in all_rows:
                    single_roww = []
                    r_in_row = row.get('R')
                    if not r_in_row:
                        for i in range(len(row['C'])):
                            if i in [0, 1, 2, 4, 5, 6]:
                                if type(row['C'][i]) != int:
                                    single_roww.append(row['C'][i])
                                else:
                                    single_roww.append(columns[i][row['C'][i]])
                            else:
                                single_roww.append(row['C'][i])

                    else:
                        r_in_row = "{:0>10b}".format(r_in_row)
                        r_in_row = r_in_row[::-1]
                        for i in range(len(r_in_row)):
                            if r_in_row[i] == '1':
                                if i in [0, 1, 2, 4, 5, 6]:
                                    if type(first_c[i]) != int:
                                        single_roww.append(first_c[i])
                                    else:
                                        single_roww.append(columns[i][first_c[i]])
                                else:
                                    single_roww.append(first_c[i])

                            else:
                                first_c[i] = row.get('C').pop(0)
                                if i in [0, 1, 2, 4, 5, 6]:
                                    if type(first_c[i]) != int:
                                        single_roww.append(first_c[i])
                                    else:
                                        single_roww.append(columns[i][first_c[i]])
                                else:
                                    single_roww.append(first_c[i])
                    temp4 = single_roww[4]
                    temp5 = single_roww[5]
                    temp7 = single_roww[7]
                    temp8 = single_roww[8]
                    temp9 = single_roww[9]
                    single_roww[4] = temp5
                    single_roww[5] = temp4
                    single_roww[7] = temp9
                    single_roww[8] = temp7
                    single_roww[9] = temp8
                    single_roww = single_roww[::-1]

                    if single_roww[5].startswith(third_last_month):
                        logger.info('End of month.')
                        return
                    else:
                        if single_roww[5].startswith(second_last_month):
                            # if single_roww[5].startswith('2023/12/11'):

                            transaction = {
                                "space": single_roww[0],
                                "price": single_roww[1],
                                "number_of_properties": single_roww[2],
                                "classification": single_roww[3],
                                "islamic_date": single_roww[4],
                                "date": single_roww[5],
                                "id": single_roww[6],
                                "city_neighborhood": single_roww[7],
                                "city": single_roww[8],
                                "region": single_roww[9]
                            }

                            print(transaction)
                            yield transaction
                        else:
                            # logger.info('Running for previous month.')
                            pass

                restart_token = data.get('results', [])[0].get('result', {}).get('data', {}).get('dsr', {}).get('DS')[
                    0].get('RT', None)

                if restart_token != None:
                    self.query['queries'][0]['Query']['Commands'][0]['SemanticQueryDataShapeCommand']['Binding'][
                        'DataReduction']['Primary']['Window']["RestartTokens"] = restart_token
                    yield scrapy.Request(method='POST', url=self.report_url, callback=self.parse, headers=self.headers,
                                         body=json.dumps(self.query), meta={'retry_times': retry_times})
                else:
                    logger.info('No Return Token')
                    return

        except Exception as e:
            logger.error(f'Parse && url is {response.url} \n :{traceback.format_ex()}')


