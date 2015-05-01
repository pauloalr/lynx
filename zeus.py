import requests
import json


class ZeusClient():

    def __init__(self, user_token, server):
        self.token = user_token
        if not server.startswith('http://'):
            self.server = 'http://' + server
        else:
            self.server = server

    def _sendRequest(self, method, path, data):
        if method == 'POST':
            r = requests.post(self.server + path, data=data)
        elif method == 'GET':
            r = requests.get(self.server + path, params=data)

        return r.status_code, r.json()

    def sendLog(self, log_name, logs):
        data = {'logs': json.dumps(logs)}
        return self._sendRequest('POST', '/logs/' + self.token + '/' + log_name + '/', data)

    def sendMetric(self, metric_name, metrics):
        data = {'metrics': json.dumps(metrics)}
        return self._sendRequest('POST', '/metrics/' + self.token + '/' + metric_name + '/', data)

    def getLog(self, log_name, pattern=None, from_date=None, to_date=None, offset=None, limit=None):
        data = {"log_name": log_name}
        if pattern:
            data['pattern'] = pattern
        if from_date:
            data['from'] = from_date
        if to_date:
            data['to'] = to_date
        if offset:
            data['offset'] = offset
        if limit:
            data['limit'] = limit

        return self._sendRequest('GET', '/logs/' + self.token + '/', data)

    def getMetric(self, metric_name=None, from_date=None, to_date=None, aggregator=None, group_interval=None, filter_condition=None, limit=None):
        data = {}
        if metric_name:
            data['metric_name'] = metric_name
        if from_date:
            data['from'] = from_date
        if to_date:
            data['to'] = to_date
        if aggregator:
            # EG. 'sum'
            data['aggregator_function'] = aggregator
        if group_interval:
            # EG. '1m'
            data['group_interval'] = group_interval
        if filter_condition:
            # EG. '"Values" < 33'
            data['filter_condition'] = filter_condition
        if limit:
            data['limit'] = limit

        return self._sendRequest('GET', '/metrics/' + self.token + '/_values/', data)

    def getMetricNames(self, metric_name=None, limit=None):
        data = {}
        if metric_name:
            data['metric_name'] = metric_name
        if limit:
            data['limit'] = limit

        return self._sendRequest('GET', '/metrics/' + self.token + '/_names/', data)
    
    