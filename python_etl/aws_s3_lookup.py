import collections.abc
import csv
# from smart_open import open
import io

import re
import zipfile
import ntpath

import boto3
import botocore
from botocore.config import Config

class S3Connect:
    __connector = None

    def __init__(self, transport_lan):
        if transport_lan:
            proxyConfig = Config(proxies={'https': 'local proxy address'})
        else:
            proxyConfig = None

        self.s3_resource = boto3.resource('s3', region_name='ap-southeast-2', config=proxyConfig)
        self.s3_client = boto3.client('s3', region_name='ap-southeast-2', config=proxyConfig)

    @classmethod
    def connector(cls, transport_lan):
        if cls.__connector is None:
            cls.__connector = S3Connect(transport_lan)

        return cls.__connector

    def object_exists(self, bucket, key):
        x = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=key)
        return x.get('KeyCount') == 1

    def restore_static_files(self, bucket, key):
        source_bucket = '-'.join(bucket.split('-')[:-1])
        source_zip_prefix = key.split(' ')[0]
        zips = self.s3_client.list_objects_v2(Bucket=source_bucket, Prefix=source_zip_prefix)
        if zips.get('KeyCount') == 0:
            raise FileNotFoundError(f'Static archive not found for {key}. Looking for prefix {source_zip_prefix} '
                                    f'in bucket {source_bucket}')
        else:
            zip_file_key = zips.get('Contents')[-1].get('Key')
            m = re.match('(.*/2[0-9]{7})T[0-9]{6}(.*GTFS)', zip_file_key)
            target_dir = None
            if m:
                target_dir = ''.join(m.groups())

            print("Unzipping {} to {}".format(zip_file_key, bucket + '/' + target_dir))
            obj = self.s3_client.get_object(Bucket=source_bucket, Key=zip_file_key)
            with io.BytesIO(obj["Body"].read()) as tf:
                # rewind the file
                tf.seek(0)
                # Read the file as a zipfile and process the members
                with zipfile.ZipFile(tf, mode='r') as zipf:
                    for file in zipf.infolist():
                        file_name = target_dir + '/' + file.filename
                        put_file = self.s3_client.put_object(
                            Bucket=bucket, Key=file_name, Body=zipf.read(file))
                        print(put_file)


class ObjectView(object):
    def __init__(self, d):
        self.__dict__ = d

    def __getitem__(self, item):
        return object.__getattribute__(self, item)


class DefaultView(object):
    def __init__(self, default_value):
        self.default_value = default_value

    def __getattribute__(self, item):
        return object.__getattribute__(self, 'default_value')

    def __getitem__(self, item):
        return object.__getattribute__(self, 'default_value')


def any_open(file_name, transport_lan):
    s3 = S3Connect.connector(transport_lan)
    bucket, key = '-', '-'
    if file_name[:3] == 's3:':
        try:
            bucket, key = file_name[5:].split('/', maxsplit=1)
            if not s3.object_exists(bucket, key):
                s3.restore_static_files(bucket, key)
            # key = '/' + key
            obj = s3.s3_resource.Object(bucket, key)
            f = obj.get()['Body'].read().decode('utf-8').split('\n')
        except:
            print(f'Could not find key ({key}) in bucket ({bucket}).')
            raise
    else:
        f = open(file_name)

    return f


def any_close(file_name, f):
    if file_name[:3] != 's3:':
        f.close()


class CSVLookup(dict):
    def __init__(self, file_name, key_field, transport_lan=False,
                 not_found_value='NOT FOUND',
                 backup_file=None):
        # self._data = dict()
        self.not_found_view = DefaultView(not_found_value)
        self._lookup_failures = dict()
        self._file_name = file_name
        self.fieldnames = None
        dict.__init__(self)

        if isinstance(key_field, collections.abc.Iterable) and not isinstance(key_field, str):
            key_field = tuple(key_field)
        else:
            key_field = tuple((key_field,))

        # Load the backup file first, so the main file overwrites the values.
        # The purpose of the backup file is to provide any values the main file doesn't have.
        if backup_file is not None:
            dict.update(self, self.load_file(backup_file, key_field, transport_lan))
        dict.update(self, self.load_file(file_name, key_field, transport_lan))

    def load_file(self, file_name, key_field, transport_lan):
        result = dict()
        f = any_open(file_name, transport_lan)
        reader = csv.DictReader(f)
        self.fieldnames = reader.fieldnames
        for k in key_field:
            if k not in reader.fieldnames:
                raise KeyError(f'Key field ({key_field}) does not exist in source file ({file_name}).')
        for line in reader:
            key = tuple(line[k] for k in key_field)
            if key not in result:
                result[key] = ObjectView(line)
            else:
                raise ValueError(f'Duplicate key ({key}) exists in source file ({file_name}).')

        any_close(file_name, f)
        return result

    def write_file(self, file_name):
        with open(file_name, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=self.fieldnames)
            writer.writeheader()
            for r in dict.values(self):
                writer.writerow(r.__dict__)

    def __getitem__(self, key):
        if isinstance(key, tuple):
            pass
        elif isinstance(key, collections.abc.Iterable) and not isinstance(key, str):
            key = tuple(key)
        else:
            key = tuple((key,))

        if dict.__contains__(self, key):
            return dict.__getitem__(self, key)
        else:
            if key not in self._lookup_failures:
                self._lookup_failures[key] = 0
            self._lookup_failures[key] += 1
            return self.not_found_view

    def errors(self):
        result = None
        if len(self._lookup_failures) > 0:
            result = f'Lookup reported failures ({ntpath.basename(self._file_name)}).\n' + \
                'Values:\n' + '\n'.join([f'- {k}  ({v} failures)' for k, v in self._lookup_failures.items()]) + '\n' + \
                'Sample keys:\n' + '\n'.join(str(x) for x in list(self.keys())[:5]) + '\n'
        return result


# def main(file_name, key_field):
#     stops = CSVLookup(file_name, key_field)

#     print(stops['200910'].stop_name)
#     # print(stops['200910'].howdy_name)

#     stop_times = CSVLookup('D:/GTFS Data/Static/20191125T231624 Static GTFS ferries/stop_times.txt',
#                            ('trip_id', 'stop_sequence'))

#     print(stop_times[('CI101-5510', '5')].arrival_time)


# if __name__ == '__main__':
#     main('D:/GTFS Data/Static/20191125T231624 Static GTFS ferries/stops.txt', 'stop_id')
