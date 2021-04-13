# -*- coding: utf-8 -*-

import configparser
import json
from csv import DictReader
from datetime import datetime
from glob import glob
from os.path import join, basename
from enum import IntEnum

import arrow
from influxdb_client import InfluxDBClient

config = configparser.ConfigParser()
config.read('config.ini')


class SpeedFan:
    '''Manages importing data from SpeedFan logs'''

    INSTALL_KEY = r'SOFTWARE\WOW6432Node\SpeedFan'

    class MetricTypes(IntEnum):
        TEMPERATURE = 1
        PWM = 2
        FAN = 3
        VOLTAGE = 4

    class Metric:
        def __init__(self, source, name, index, metric_type, units):
            self.source = source
            self.name = name
            self.index = index
            self.metric_type = metric_type
            self.units = units
            self.tags = {
                'name': self.name,
                'source': self.source,
                'index': self.index,
                'units': self.units
            }
            self.measurements = {}

        def __str__(self):
            return f'{self.name} ({self.metric_type} {self.index} on {self.source})'

        def to_influx(self):
            '''Returns an InfluxDB-compatible list of measurements'''
            output = []
            for time, measurement in self.measurements.items():
                datapoint = {
                    'measurement': self.name,
                    'tags': self.tags,
                    'time': time.to('utc').format('YYYY-MM-DDTHH:mm:ss[Z]'),
                    'fields': {
                        'value': measurement
                    }
                }
                output.append(datapoint)
            return output

    class Temp(Metric):
        def __init__(self, source, name, index, units, wanted=None, warning=None, offset=0, used_pwms=0):
            super().__init__(source, name, index, SpeedFan.MetricTypes.TEMPERATURE, units)
            self.wanted = wanted
            self.warning = warning
            self.offset = offset
            self.used_pwms = used_pwms
            self.tags['wanted'] = self.wanted
            self.tags['warning'] = self.warning
            self.tags['offset'] = self.offset
            self.tags['used_pwms'] = self.used_pwms

    class PWM(Metric):
        def __init__(self, source, name, index, minimum=0, maximum=100, variate=False):
            super().__init__(source, name, index, SpeedFan.MetricTypes.PWM, '%')
            self.minimum = minimum
            self.maximum = maximum
            self.variate = variate
            self.tags['minimum'] = self.minimum
            self.tags['maximum'] = self.maximum
            self.tags['variate'] = self.variate

    class Fan(Metric):
        def __init__(self, source, name, index):
            super().__init__(source, name, index, SpeedFan.MetricTypes.FAN, 'RPM')

    class Volt(Metric):
        def __init__(self, source, name, index):
            super().__init__(source, name, index, SpeedFan.MetricTypes.VOLTAGE, 'V')

    def __init__(self, install_dir=None):
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
                        minimum=params['minimum'],
                        maximum=params['maximum'],
                        variate=params['variate']
                    )
                elif metric_type == 'Fan':
                    metric = SpeedFan.Fan(
                        source=source,
                        name=name,
                        index=index
                    )
                elif metric_type == 'Volt':
                    metric = SpeedFan.Volt(
                        source=source,
                        name=name,
                        index=index
                    )

                self.metrics[name] = metric

    def parse_logs(self):
        logfiles = glob(join(self._dir, 'SFLog*.csv'))
        for logfile in logfiles:
            logtime = arrow.get(basename(logfile)[5:13], 'YYYYMMDD').replace(
                tzinfo=self.tzinfo)
            if self.log_has_header:
                logs = DictReader(open(logfile), delimiter='\t')
            else:
                logs = DictReader(open(logfile), delimiter='\t',
                                  fieldnames=self.header)
            for log in logs:
                timestamp = logtime.shift(seconds=int(log['Seconds']))
                for name in self.header[1:]:
                    if self.metrics[name].metric_type == SpeedFan.MetricTypes.TEMPERATURE:
                        self.metrics[name].measurements[timestamp] = float(
                            log[name])
                            
                    elif self.metrics[name].metric_type == SpeedFan.MetricTypes.VOLTAGE:
                        self.metrics[name].measurements[timestamp] = float(
                            log[name])
                            
                    elif self.metrics[name].metric_type == SpeedFan.MetricTypes.FAN:
                        self.metrics[name].measurements[timestamp] = int(
                            log[name])
                            
                    elif self.metrics[name].metric_type == SpeedFan.MetricTypes.PWM:
                        self.metrics[name].measurements[timestamp] = float(
                            log[name])


if __name__ == '__main__':
    speedfan = SpeedFan('E:\\')
    print(speedfan.header)
    speedfan.parse_logs()
    for name,metric in speedfan.metrics.items():
        print(f'{name}: {len(metric.measurements)} observations')
        influxd = metric.to_influx()
        print(influxd[0])
