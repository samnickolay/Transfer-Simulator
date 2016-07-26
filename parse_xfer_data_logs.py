
import datetime
from enum import Enum

from os.path import isfile
import sys


column_evals = {
    'id': 'int("{}")',
    'ip_address': 'str("{}")',
    'start_time': 'datetime.datetime.strptime("{}", "%Y-%m-%d %H:%M:%S.%f")',
    'transfer_time': 'datetime.datetime.strptime("{}", "%H:%M:%S.%f")',
    'trans_type': 'int("{}")',
    'num_bytes': 'int("{}")'
}


class Transfer(object):
    def __init__(self, transfer_id, ip_address, start_time, transfer_time, num_bytes, trans_type):
        self.transfer_id = transfer_id
        self.ip_address = ip_address
        self.trans_type = trans_type

        self.requested_start_time = start_time
        self.requested_transfer_time = transfer_time
        self.requested_end_time = start_time + transfer_time

        self.total_bytes = num_bytes
        self.bytes_left = num_bytes

        self.requested_rate = float(self.total_bytes) / self.requested_transfer_time.total_seconds()
        self.current_rate = self.requested_rate

        self.start_time = None
        self.end_time = None
        self.transfer_time = None

    def start_transfer(self, current_time, rate=None):
        if rate is None:
            self.transfer_time = self.requested_transfer_time
        else:
            self.current_rate = rate
            self.transfer_time = datetime.timedelta(seconds=(float(self.total_bytes) / self.current_rate))

        self.start_time = current_time
        self.end_time = self.start_time + self.transfer_time

    def update_rate(self, rate, current_time):
        self.current_rate = rate
        self.end_time = current_time + datetime.timedelta(seconds=(float(self.bytes_left) / self.current_rate))
        self.transfer_time = self.end_time - self.start_time

    def bytes_transferred_during_interval(self, interval):
        # figure out how the transfer intersects with interval
        start_t = max(self.start_time, interval.start_time)
        end_t = min(self.end_time, interval.end_time)
        bytes_transferred = (end_t - start_t).total_seconds() * self.current_rate
        return bytes_transferred

    def update_bytes_for_interval(self, interval):
        self.bytes_left -= self.bytes_transferred_during_interval(interval)

    def __repr__(self):
        if self.start_time is not None:
            return "( Requested Time {} - {} | Actual Time {} - {} | {} Total Bytes | {} Bytes Remaining )".\
                format(self.requested_start_time, self.requested_end_time,
                       self.start_time, self.end_time, self.total_bytes, self.bytes_left)
        else:
            return "( Requested Time {} - {} | {} Total Bytes | {} Bytes Remaining )". \
                format(self.requested_start_time, self.requested_end_time, self.total_bytes, self.bytes_left)


class TransferType(Enum):
    OD = 0
    BE = 1


def parse_logs(file_name):
    if not isfile(file_name):
        print("ERROR - Provided file_name for xfer log file is not valid: '{}'".format(file_name))
        sys.exit(1)
    with open(file_name, 'r') as file_in:

        # read headers from the first row
        first_line = file_in.readline()
        column_headers = [val.strip() for val in first_line.split('|')]

        # make sure all of the necessary headers are there so parsing will work
        for key, value in column_evals.items():
            if key not in column_headers:
                print("ERROR - Provided xfer log file does not contain all of the necessary columns")
                print("'{}' does not contain column '{}'".format(file_name, key))
                sys.exit(1)

        bad_rows = 0
        transfers = []

        for line in file_in:
            line = line.strip()
            line_values = [val.strip() for val in line.split('|')]

            if line_values is None or ''.join(line_values) is "" or line.strip('-+') is '':
                print('skipped line: {}'.format(line))
                continue
            try:
                row = {}
                for idx, value in enumerate(line_values):
                    if value == '' or value == 'NULL':
                        continue
                    column_header = column_headers[idx]
                    if column_header in column_evals:
                        row[column_header] = eval(column_evals[column_header].format(value))
                    else:
                        row[column_header] = value

                # convert the datetime created for 'transfer_time' to a timedelta
                min_time = datetime.datetime.strptime('0', "%S")
                row['transfer_time'] = row['transfer_time'] - min_time

                # add extra usual items to the transfer
                row['end_time'] = row['start_time'] + row['transfer_time']
                # row['rate'] = float(row['num_bytes']) / row['transfer_time'].total_seconds()

                new_transfer = Transfer(row['id'], row['ip_address'], row['start_time'], row['transfer_time'],
                                        row['num_bytes'], row['trans_type'])
                transfers.append(new_transfer)

            except Exception:
                # if the line is the last line '(123 rows)', don't print error message
                if line.strip().find('rows)') is not -1:
                    # num_rows = line.split()[1:].split(' ')
                    print('{} in log file - {} bad rows'.format(line.strip('()'), bad_rows))

                else:
                    # print("Could not parse column {} ({}) in this row:".format(idx, column_headers[idx]))
                    print("Could not parse column {} ('{}') in this row:".format(column_headers[idx], line_values[idx]))
                    print(line)
                    bad_rows += 1

    return transfers
