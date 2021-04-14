# -*- coding: utf-8 -*-

import configparser
import json
from csv import DictReader
from datetime import datetime
from enum import IntEnum
from glob import glob
from numbers import Number
from os.path import basename, join
from socket import gethostname
from typing import Sequence

import arrow
from influxdb import InfluxDBClient

config = configparser.ConfigParser()
config.read('config.ini')


class InfluxClient:
    '''Manages an InfluxDB connection'''

    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.client = InfluxDBClient(
            host=self.host,
            port=self.port,
            username=self.user,
            password=self.password,
            database=self.database
        )

    def write_to_influx(self, packet: Sequence[dict]):
        '''Write a list of dicts to the database'''
        if type(packet) == dict:
            packet = [packet]
        self.client.write_points(packet)


class SpeedFan:
    '''Manages importing data from SpeedFan logs'''

    INSTALL_KEY = r'SOFTWARE\WOW6432Node\SpeedFan'

    class MetricTypes(IntEnum):
        TEMPERATURE = 1
        PWM = 2
        FAN = 3
        VOLTAGE = 4

    class Metric:
        def __init__(self, source: str, name: str, index: int, metric_type: IntEnum, units: str, hostname: str):
            self.source = source
            self.name = name
            self.index = index
            self.metric_type = metric_type
            self.units = units
            self.hostname = hostname
            self.tags = {
                'metric': self.name,
                'chipset': self.source,
                'index': self.index,
                'host': self.hostname
            }

        def __str__(self) -> str:
            return f'{self.name} ({self.metric_type} {self.index} on {self.source})'

        def to_influx(self, timestamp: arrow, measurement: Number) -> dict:
            '''Returns an InfluxDB-compatible measurement'''
            datapoint = {
                'measurement': self.units,
                'tags': self.tags,
                'time': timestamp.to('utc').format('YYYY-MM-DDTHH:mm:ss[Z]'),
                'fields': {
                    'value': measurement
                }
            }
            return datapoint

    class Temp(Metric):
        def __init__(self, source: str, name: str, index: int, units: str, hostname: str, wanted: Number = None, warning: Number = None, offset: Number = 0, used_pwms: Number = 0):
            super().__init__(source, name, index, SpeedFan.MetricTypes.TEMPERATURE, units, hostname)
            self.wanted = wanted
            self.warning = warning
            self.offset = offset
            self.used_pwms = used_pwms
            self.tags['wanted'] = self.wanted
            self.tags['warning'] = self.warning
            self.tags['offset'] = self.offset
            self.tags['used_pwms'] = self.used_pwms

    class PWM(Metric):
        def __init__(self, source: str, name: str, index: int, hostname: str, minimum: Number = 0, maximum: Number = 100, variate: bool = False):
            super().__init__(source, name, index, SpeedFan.MetricTypes.PWM, '%', hostname)
            self.minimum = minimum
            self.maximum = maximum
            self.variate = variate
            self.tags['minimum'] = self.minimum
            self.tags['maximum'] = self.maximum
            self.tags['variate'] = self.variate

    class Fan(Metric):
        def __init__(self, source: str, name: str, index: int, hostname: str):
            super().__init__(source, name, index, SpeedFan.MetricTypes.FAN, 'RPM', hostname)

    class Volt(Metric):
        def __init__(self, source: str, name: str, index: int, hostname: str):
            super().__init__(source, name, index, SpeedFan.MetricTypes.VOLTAGE, 'V', hostname)

    def __init__(self, install_dir=None, hostname=None):
        if install_dir is None:
            self._dir = self._get_dir()
        else:
            self._dir = install_dir
        self._params = self._get_params()
        self.temp_units = ('°F', '°C')[
            self._params.getboolean('speedfan', 'UseCelsius')]
        self.log_has_header = self._params.getboolean(
            'speedfan', 'LogAddHeader')
        self.tzinfo = datetime.now().astimezone().tzinfo
        self.metrics = {}
        if hostname is None:
            hostname = gethostname()
        self.hostname = hostname
        self.header = ['Seconds']
        self._get_metrics()

    def _get_dir(self) -> str:
        '''Return the install directory of SpeedFan from the Windows Registry'''
        import winreg
        hndl = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, self.INSTALL_KEY)
        return winreg.QueryValue(hndl, None)

    def _get_params(self) -> configparser.ConfigParser:
        '''Return a ConfigParser for the SpeedFan program options, with one section called 'speedfan' '''
        params = '[speedfan]\n' + \
            open(join(self._dir, 'speedfanparams.cfg')).read()
        speedfan = configparser.ConfigParser()
        speedfan.read_string(params)
        return speedfan

    def _get_metrics(self):
        '''Record which metrics SpeedFan is logging'''
        sens_blocks = open(join(self._dir, 'speedfansens.cfg')).read().split(
            'xxx the end')[1].strip().split('xxx end')
        for block in sens_blocks:
            if not block:
                continue
            header, *param_block = block.replace('xxx', '').strip().split('\n')

            try:
                metric_index, source = header.split(' from ')
            except ValueError:
                print(f'{header}:{block}')
                raise
            metric_type, index = metric_index.split(' ')
            index = int(index)

            params = {}
            for param in param_block:
                key, value = param.split('=')
                if value == 'true':
                    value = True
                elif value == 'false':
                    value = False
                else:
                    try:
                        value = int(value)
                    except ValueError:
                        pass
                params[key] = value

            if params['active'] and params['logged']:
                # metric is logged and active, enable exporting

                # add name to expected headers
                name = params['name']
                self.header.append(name)

                # create metric of the right type
                if metric_type == 'Temp':
                    metric = SpeedFan.Temp(
                        source=source,
                        name=name,
                        index=index,
                        units=self.temp_units,
                        hostname=self.hostname,
                        wanted=params['wanted'],
                        warning=params['warning'],
                        offset=params['offset'],
                        used_pwms=params['UsedPwms']
                    )
                elif metric_type == 'Pwm':
                    metric = SpeedFan.PWM(
                        source=source,
                        name=name,
                        index=index,
                        hostname=self.hostname,
                        minimum=params['minimum'],
                        maximum=params['maximum'],
                        variate=params['variate']
                    )
                elif metric_type == 'Fan':
                    metric = SpeedFan.Fan(
                        source=source,
                        name=name,
                        index=index,
                        hostname=self.hostname
                    )
                elif metric_type == 'Volt':
                    metric = SpeedFan.Volt(
                        source=source,
                        name=name,
                        index=index,
                        hostname=self.hostname
                    )

                self.metrics[name] = metric

    def find_last(self, client: InfluxClient):
        '''Locate the last data in the database and return an Arrow'''
        results = client.client.query(
            'SELECT LAST(value) FROM "°C","RPM","%","V" WHERE host=$host', bind_params={'host': self.hostname})
        last = arrow.get(0)
        for series in results.raw['series']:
            series_last = arrow.get(series['values'][0][0]).to(self.tzinfo)
            if last < series_last:
                last = series_last
        return last

    def parse_logs(self, client: InfluxClient):
        '''Loop through all logs and write the data to Influx'''
        logfiles = glob(join(self._dir, 'SFLog*.csv'))
        last = self.find_last(client)
        for logfile in logfiles:
            logtime = arrow.get(basename(logfile)[5:13], 'YYYYMMDD').replace(
                tzinfo=self.tzinfo)
            if logtime.date() < last.date():
                # old logfile from before the date of the last log in database, skip
                print(f'Skipping {logfile}, older than {last}')
                continue
            if self.log_has_header:
                logs = DictReader(open(logfile), delimiter='\t')
            else:
                logs = DictReader(open(logfile), delimiter='\t',
                                  fieldnames=self.header)
            for log in logs:
                timestamp = logtime.shift(seconds=int(log['Seconds']))
                if timestamp < last:
                    # old row that is already in the database, skip
                    continue
                for name in self.header[1:]:
                    if self.metrics[name].metric_type == SpeedFan.MetricTypes.TEMPERATURE:
                        packet = self.metrics[name].to_influx(
                            timestamp, float(log[name]))

                    elif self.metrics[name].metric_type == SpeedFan.MetricTypes.VOLTAGE:
                        packet = self.metrics[name].to_influx(
                            timestamp, float(log[name]))

                    elif self.metrics[name].metric_type == SpeedFan.MetricTypes.FAN:
                        packet = self.metrics[name].to_influx(
                            timestamp, int(log[name]))

                    elif self.metrics[name].metric_type == SpeedFan.MetricTypes.PWM:
                        packet = self.metrics[name].to_influx(
                            timestamp, float(log[name]))
                    client.write_to_influx(packet)


if __name__ == '__main__':
    influx = InfluxClient(
        config.get('database', 'host'),
        config.getint('database', 'port'),
        config.get('database', 'user'),
        config.get('database', 'password'),
        config.get('database', 'database')
    )

    speedfan = SpeedFan()
    print(speedfan.header)
    speedfan.parse_logs(influx)
