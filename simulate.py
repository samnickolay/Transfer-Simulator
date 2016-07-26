import datetime
from operator import attrgetter
import numpy

from parse_xfer_data_logs import TransferType
import copy
import random

from os.path import exists
from os import makedirs

import make_plot


class Interval(object):
    def __init__(self, start_time, interval_length):
        self.start_time = start_time
        self.length = interval_length
        self.end_time = start_time + interval_length
        # self.end_time = start_time + interval_length - datetime.timedelta(microseconds=1)
        self.bytes = 0

        self.OD_transfers = []
        self.BE_transfers = []
        self.OD_bytes = 0
        self.BE_bytes = 0

    def add_transfer(self, transfer):
        # add the transfer to the correct transfer list for the interval and
        # increase the interval's bytes by the number of bytes the transfer transfers during the interval
        if transfer.trans_type is TransferType.BE:
            self.BE_transfers.append(transfer)
            self.BE_bytes += transfer.bytes_transferred_during_interval(self)
        elif transfer.trans_type is TransferType.OD:
            self.OD_transfers.append(transfer)
            self.OD_bytes += transfer.bytes_transferred_during_interval(self)

        # self.bytes += transfer.update_bytes(self)

    # recalculate the number of bytes for BE transfers if one of the transfers was modified
    def update_BE_network_load(self):
        self.BE_bytes = 0
        for transfer in self.BE_transfers:
            self.BE_bytes += transfer.bytes_transferred_during_interval(self)

    # recalculate the number of bytes for BE transfers if one of the transfers was modified
    def update_OD_network_load(self):
        self.OD_bytes = 0
        for transfer in self.OD_transfers:
            self.BE_bytes += transfer.bytes_transferred_during_interval(self)

    def network_load(self):
        return float(self.OD_bytes + self.BE_bytes) / self.length.total_seconds()
        # return self.bytes / self.length.total_seconds()

    def log_header(self):
        return 'start_time, end_time, length, bytes, # OD_transfers, # BE_transfers\n'

    def save_to_log(self):
        return '{}, {}, {}, {}, {}, {}\n'.format(self.start_time, self.end_time, self.length, self.bytes,
                                                 len(self.OD_transfers), len(self.BE_transfers))

    def __repr__(self):
        return "(start_t: {}, end_t: {}, bytes: {})".format(self.start_time, self.end_time, self.bytes)


def prepare_simulation(transfers, interval_length, date, date_range, file_name, heuristic_tup):
    heuristic_name, heuristic_function = heuristic_tup

    # misc. setup work for the simulation
    file_name = file_name[file_name.rindex('/')+1:]

    interval_stat_str = 'Interval Statistics for {} - {} Transfers \nIntervals Between {} - {}'. \
        format(file_name, len(transfers), date_range[0], date_range[1])

    plot_title = "{} - {} Heuristic on {} - {} Transfers".format(file_name, date, len(transfers))

    plots_folder = 'plots-xfer_data_logs'
    # verify that the plots output folder exists, if it doesn't, then create it
    if not exists(plots_folder):
        makedirs(plots_folder)

    log_folder = 'output_logs'
    # verify that the folder exists, if it doesn't, then create it
    if not exists(log_folder):
        makedirs(log_folder)

    # run the simulation using all of the transfers as OD
    tmp_transfers = copy.deepcopy(transfers)
    # label all of the tmp_transfers as OD:
    for transfer in tmp_transfers:
        transfer.trans_type = TransferType.OD

    original_intervals = simulate(interval_length, date, tmp_transfers, [], 0, heuristic_function)

    mean, std, median = get_interval_statistics(original_intervals, 'Original ' + interval_stat_str)

    # plot the resulting intervals
    plot_filename = "{}/{}_{}_{}-transfers_original_{}.png". \
        format(plots_folder, file_name, date, len(transfers), heuristic_name)
    make_plot.plot_intervals(plot_filename, plot_title, [('Original', original_intervals)])

    # OD_transfer_percentages = [0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
    OD_transfer_percentages = [0.1, 0.3, 0.5, 0.7, 0.9]
    for OD_percentage in OD_transfer_percentages:
        tmp_transfers = copy.deepcopy(transfers)

        # shuffle the temp transfer list so we can randomly divide the list
        random.shuffle(tmp_transfers)

        # calculate the number of OD and BE transfers and make the appropriate lists
        OD_transfer_count = round(len(tmp_transfers) * OD_percentage)

        OD_transfers = tmp_transfers[:OD_transfer_count]
        # label all of the OD_transfers as OD:
        for transfer in OD_transfers:
            transfer.trans_type = TransferType.OD

        BE_transfers = tmp_transfers[OD_transfer_count:]
        # label all of the BE_transfers as BE:
        for transfer in BE_transfers:
            transfer.trans_type = TransferType.BE

        net_cap = mean * 2
        new_intervals = simulate(interval_length, date, OD_transfers, BE_transfers, net_cap, heuristic_function)

        get_interval_statistics(new_intervals, 'OD percentage: {} - '.format(OD_percentage) + interval_stat_str)

        # plot the resulting intervals
        plot_filename = "{}/{}/{}_{}_{}-transfers_{:.2f}-OD_{}.png". \
            format(plots_folder, heuristic_name, file_name, date, len(transfers), OD_percentage, heuristic_name)
        intervals_list = [('Original', original_intervals), ('{}% OD'.format(OD_percentage*100), new_intervals)]

        make_plot.plot_intervals(plot_filename, plot_title, intervals_list)

        # save the interval data to log
        # log_file = "{}/{}_{}_{}-transfers_{:.2f}-OD_percentage.csv". \
        #     format(log_folder, file_name, date, len(transfers), OD_percentage)
        #
        # with open(log_file, 'w') as the_file:
        #     the_file.write( new_intervals[0].log_header())
        #     for interval in new_intervals:
        #         the_file.write(interval.save_to_log())


def simulate(interval_length, date, OD_transfers, BE_transfers, network_capacity, heuristic):

    # make intervals to use for the simulation
    intervals = make_intervals_given_transfers(interval_length, date, OD_transfers, BE_transfers)
    # intervals = make_intervals(interval_length, interval_start_time, interval_end_time)

    unqueued_OD = sorted(OD_transfers, key=attrgetter('requested_start_time'))
    unqueued_BE = sorted(BE_transfers, key=attrgetter('requested_start_time'))
    queued_OD = []
    queued_BE = []

    interval_idx = -1
    current_interval = intervals[0]

    # iterate until all of the transfers have been completely simulated
    while len(unqueued_OD) > 0 or len(unqueued_BE) or len(queued_OD) > 0 or len(queued_BE) > 0 or\
            len(current_interval.BE_transfers) > 0 or len(current_interval.OD_transfers) > 0:

        interval_idx += 1
        if interval_idx >= len(intervals):
            add_interval(intervals)

        previous_interval = current_interval
        current_interval = intervals[interval_idx]

        # only add OD_transfers that are still transferring during this interval
        for transfer in previous_interval.OD_transfers:
            if transfer.end_time > current_interval.start_time:
                current_interval.add_transfer(transfer)

        # only add BE_transfers that are still transferring during this interval
        for transfer in previous_interval.BE_transfers:
            if transfer.end_time > current_interval.start_time:
                current_interval.add_transfer(transfer)

        # add any unqueued_OD_transfers to the queued list
        while len(unqueued_OD) > 0 and \
                        unqueued_OD[0].requested_start_time < current_interval.start_time:
            queued_OD.append(unqueued_OD.pop(0))

        # add any unqueued_BE_transfers to the queued list
        while len(unqueued_BE) > 0 and\
                        unqueued_BE[0].requested_start_time < current_interval.start_time:
            queued_BE.append(unqueued_BE.pop(0))

        # # for job in OD job queue, run job
        # while len(queued_OD) > 0:
        #     transfer = queued_OD.pop(0)
        #     transfer.start_transfer(current_interval.start_time)
        #
        #     current_interval.add_transfer(transfer)
        #
        # # for job in BE job queue, if sum of all running jobs < 0.95 * capacity, run job
        # while len(queued_BE) > 0 and current_interval.network_load() < 0.95 * network_capacity:
        #     transfer = queued_BE.pop(0)
        #     transfer.start_transfer(current_interval.start_time)
        #     current_interval.add_transfer(transfer)

        # run the new transfers based on the current heuristic
        heuristic(current_interval, queued_OD, queued_BE, network_capacity)

        # update all the transfers by subtracting the bytes transferred during the current interval
        for transfer in current_interval.OD_transfers:
            transfer.update_bytes_for_interval(current_interval)
        for transfer in current_interval.BE_transfers:
            transfer.update_bytes_for_interval(current_interval)

        # Set bytes for current_interval now that we're done with it
        current_interval.bytes = current_interval.OD_bytes + current_interval.BE_bytes

    # Once the simulation is done, trim the intervals if required
    intervals = trim_intervals(intervals, date)

    return intervals


# the baseline heuristic for running transfers
def baseline_heuristic(current_interval, queued_OD, queued_BE, network_capacity):
    # for job in OD job queue, run job
    while len(queued_OD) > 0:
        transfer = queued_OD.pop(0)
        transfer.start_transfer(current_interval.start_time)
        current_interval.add_transfer(transfer)

    # for job in BE job queue, if sum of all running jobs < 0.95 * capacity, run job
    while len(queued_BE) > 0 and current_interval.network_load() < 0.95 * network_capacity:
        transfer = queued_BE.pop(0)
        transfer.start_transfer(current_interval.start_time)
        current_interval.add_transfer(transfer)


# the baseline heuristic for running transfers
def FCFS_heuristic(current_interval, queued_OD, queued_BE, network_capacity):
    # for job in OD job queue, run job
    BE_limiting = False
    while len(queued_OD) > 0:
        if current_interval.network_load() > 0.95 * network_capacity and BE_limiting is False:
            BE_limiting = True
            for transfer in current_interval.BE_transfers:
                # arbitrarily set minimum rate to 100 bytes/sec
                limiting_rate = 100
                transfer.update_rate(limiting_rate)
            current_interval.update_BE_network_load()

        transfer = queued_OD.pop(0)
        transfer.start_transfer(current_interval.start_time)
        current_interval.add_transfer(transfer)

    for transfer in current_interval.BE_transfers:
        available_bandwidth = 0.95 * network_capacity - current_interval.network_load()
        if available_bandwidth > 0 and transfer.current_rate < transfer.requested_rate:
            new_rate = min(transfer.current_rate + available_bandwidth, transfer.requested_rate)
            transfer.update_rate(new_rate)

        current_interval.update_BE_network_load()

    # for job in BE job queue, if sum of all running jobs < 0.95 * capacity, run job
    while len(queued_BE) > 0 and current_interval.network_load() < 0.95 * network_capacity:
        transfer = queued_BE.pop(0)
        transfer.start_transfer(current_interval.start_time)
        current_interval.add_transfer(transfer)


def trim_intervals(original_intervals, date):
    date_time = datetime.datetime(year=date.year, month=date.month, day=date.day)

    trimmed_intervals = []
    for interval in original_intervals:
        if interval.start_time < date_time:
            continue
        elif interval.start_time >= date_time + datetime.timedelta(days=1):
            break
        else:
            trimmed_intervals.append(interval)
    return trimmed_intervals


def add_interval(intervals):
    new_interval = Interval(intervals[-1].start_time + intervals[-1].length, intervals[-1].length)
    intervals.append(new_interval)


def make_intervals_given_transfers(interval_length, date, OD_transfers, BE_transfers):
    # make intervals to use for the simulation
    date_time = datetime.datetime(year=date.year, month=date.month, day=date.day)

    interval_start_time = date_time
    interval_end_time = date_time + datetime.timedelta(days=1)

    if len(OD_transfers) > 0:
        while OD_transfers[0].requested_start_time < interval_start_time:
            interval_start_time -= interval_length
        while OD_transfers[-1].requested_end_time > interval_end_time:
            interval_end_time += interval_length

    if len(BE_transfers) > 0:
        while BE_transfers[0].requested_start_time < interval_start_time:
            interval_start_time -= interval_length
        while BE_transfers[-1].requested_end_time > interval_end_time:
            interval_end_time += interval_length

    intervals = make_intervals(interval_length, interval_start_time, interval_end_time)
    return intervals


def make_intervals(interval_length, start_time, end_time):
    intervals = []
    cur_start_time = start_time

    while cur_start_time < end_time:
        intervals.append(Interval(cur_start_time, interval_length))
        cur_start_time += interval_length

    return intervals


def get_interval_statistics(intervals, printer=None):
    interval_length = intervals[0].length.total_seconds()
    bytes_per_megabyte = 1024 * 1024
    interval_rates = [float(interval.bytes) / interval_length for interval in intervals]

    mean_value = numpy.mean(interval_rates)
    std_deviation = numpy.std(interval_rates)
    median_value = numpy.median(interval_rates)

    if printer is not None:
        print('\n' + printer)
        print("mean Interval value: {} MiB/Second".format(mean_value / bytes_per_megabyte))
        print("median Interval value: {} MiB/Second".format(median_value / bytes_per_megabyte))
        print("std deviation: {}".format(std_deviation / bytes_per_megabyte))

    return mean_value, std_deviation, median_value
