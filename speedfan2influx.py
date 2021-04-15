# -*- coding: utf-8 -*-

import configparser
from csv import DictReader
from datetime import datetime
from glob import glob
from os.path import basename, join
from socket import gethostname

import arrow
from arrow.arrow import Arrow
from influxdb import InfluxDBClient, SeriesHelper

config = configparser.ConfigParser()
config.read('config.ini')


class SpeedFan:
    '''Manages importing data from SpeedFan logs'''

    INSTALL_KEY = r'SOFTWARE\WOW6432Node\SpeedFan'

    class Temp(SeriesHelper):
        class Meta:
            client = None
            series_name = '°C'
            fields = ['value']
            time_precision = 's'
            tags = [
                'metric',
                'chipset',
                'index',
                'host',
                'wanted',
                'warning',
                'offset',
                'used_pwms'
            ]
            bulk_size = 100
            autocommit = True

    class PWM(SeriesHelper):
        class Meta:
            client = None
            series_name = '%'
            fields = ['value']
            time_precision = 's'
            tags = [
                'metric',
                'chipset',
                'index',
                'host',
                'minimum',
                'maximum',
                'variate'
            ]
            bulk_size = 100
            autocommit = True

    class Fan(SeriesHelper):
        class Meta:
            client = None
            series_name = 'RPM'
            fields = ['value']
            time_precision = 's'
            tags = [
                'metric',
                'chipset',
                'index',
                'host'
            ]
            bulk_size = 100
            autocommit = True

    class Volt(SeriesHelper):
        class Meta:
            client = None
            series_name = 'V'
            fields = ['value']
            time_precision = 's'
            tags = [
                'metric',
                'chipset',
                'index',
                'host'
            ]
            bulk_size = 100
            autocommit = True

    def __init__(self, install_dir=None, hostname=None):
        if install_dir is None:
            self._dir = self._get_dir()
        else:
            self._dir = install_dir
        self._params = self._get_params()
        self.temp_units = ('°F', '°C')[
            self._params.getboolean('speedfan', 'UseCelsius')]
        SpeedFan.Temp.Meta.series_name = self.temp_units
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

    def _parse_metric_block(self, block: str) -> tuple:
        '''Process a metric block to return its contents'''
        header, *param_block = block.replace('xxx', '').strip().split('\n')

        metric_index, source = header.split(' from ')
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
        return source, metric_type, index, params

    def _get_metrics(self):
        '''Record which metrics SpeedFan is logging'''
        sens_blocks = open(join(self._dir, 'speedfansens.cfg')).read().split(
            'xxx the end')[1].strip().split('xxx end')
        for block in sens_blocks:
            if not block:
                continue
            source, metric_type, index, params = self._parse_metric_block(
                block)

            if params['active'] and params['logged']:
                # metric is logged and active, enable exporting

                # add name to expected headers
                name = params['name']
                self.header.append(name)

                # create metric of the right type
                if metric_type == 'Temp':
                    metric = {
                        'type': 'temp',
                        'function': float,
                        'chipset': source,
                        'metric': name,
                        'index': index,
                        'units': self.temp_units,
                        'hostname': self.hostname,
                        'wanted': params['wanted'],
                        'warning': params['warning'],
                        'offset': params['offset'],
                        'used_pwms': params['UsedPwms']
                    }
                elif metric_type == 'Pwm':
                    metric = {
                        'type': 'pwm',
                        'function': float,
                        'chipset': source,
                        'metric': name,
                        'index': index,
                        'hostname': self.hostname,
                        'minimum': params['minimum'],
                        'maximum': params['maximum'],
                        'variate': params['variate']
                    }
                elif metric_type == 'Fan':
                    metric = {
                        'type': 'fan',
                        'function': int,
                        'chipset': source,
                        'metric': name,
                        'index': index,
                        'hostname': self.hostname
                    }
                elif metric_type == 'Volt':
                    metric = {
                        'type': 'volt',
                        'function': float,
                        'chipset': source,
                        'metric': name,
                        'index': index,
                        'hostname': self.hostname
                    }

                self.metrics[name] = metric

    def find_last(self, client: InfluxDBClient) -> Arrow:
        '''Locate the last data in the database and return it as an Arrow'''
        results = client.query(
            'SELECT LAST(value) FROM "°C","°F","RPM","%","V" WHERE host=$host', bind_params={'host': self.hostname})
        last = arrow.get(0)
        for series in results.raw['series']:
            series_last = arrow.get(series['values'][0][0]).to(self.tzinfo)
            if last < series_last:
                last = series_last
        return last

    def parse_logs(self, client: InfluxDBClient):
        '''Loop through all logs and write the data to Influx'''

        SpeedFan.Temp.Meta.client = client
        SpeedFan.PWM.Meta.client = client
        SpeedFan.Fan.Meta.client = client
        SpeedFan.Volt.Meta.client = client

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
                    value = self.metrics[name]['function'](log[name])
                    dt = timestamp.to('utc').format('YYYY-MM-DDTHH:mm:ss[Z]')
                    if self.metrics[name]['type'] == 'temp':
                        SpeedFan.Temp(
                            host=self.metrics[name]['hostname'],
                            chipset=self.metrics[name]['chipset'],
                            index=self.metrics[name]['index'],
                            metric=self.metrics[name]['metric'],
                            value=value,
                            time=dt,
                            wanted=self.metrics[name]['wanted'],
                            warning=self.metrics[name]['warning'],
                            offset=self.metrics[name]['offset'],
                            used_pwms=self.metrics[name]['used_pwms']
                        )

                    elif self.metrics[name]['type'] == 'pwm':
                        SpeedFan.PWM(
                            host=self.metrics[name]['hostname'],
                            chipset=self.metrics[name]['chipset'],
                            index=self.metrics[name]['index'],
                            metric=self.metrics[name]['metric'],
                            value=value,
                            time=dt,
                            minimum=self.metrics[name]['minimum'],
                            maximum=self.metrics[name]['maximum'],
                            variate=self.metrics[name]['variate']
                        )

                    elif self.metrics[name]['type'] == 'fan':
                        SpeedFan.Fan(
                            host=self.metrics[name]['hostname'],
                            chipset=self.metrics[name]['chipset'],
                            index=self.metrics[name]['index'],
                            metric=self.metrics[name]['metric'],
                            value=value,
                            time=dt
                        )

                    elif self.metrics[name]['type'] == 'volt':
                        SpeedFan.Volt(
                            host=self.metrics[name]['hostname'],
                            chipset=self.metrics[name]['chipset'],
                            index=self.metrics[name]['index'],
                            metric=self.metrics[name]['metric'],
                            value=value,
                            time=dt
                        )


if __name__ == '__main__':
    influx = InfluxDBClient(
        host=config.get('database', 'host'),
        port=config.getint('database', 'port'),
        username=config.get('database', 'user'),
        password=config.get('database', 'password'),
        database=config.get('database', 'database')
    )

    speedfan = SpeedFan()
    print(speedfan.header)
    speedfan.parse_logs(influx)
