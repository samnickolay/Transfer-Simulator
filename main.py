import sys
from os.path import isfile
import datetime

import parse_xfer_data_logs
import simulate


def main():
    '''Main function'''

    if len(sys.argv) < 3 or len(sys.argv) > 3:
        print("Received %d arguments - Expected 2 (log file filename, and date to simulate)" % (len(sys.argv) - 1))
        print('Example: xfer_data_logs/128.142.18.166.xfer 2013-5-3')
        raise SystemExit

    file_name = sys.argv[1]
    if isfile(file_name):
        file_name = file_name
    else:
        print("Input string is not a valid file_name - %s" % (file_name))
        raise SystemExit

    # plots_folder = 'plots-xfer_data_logs'
    # # verify that the plots output folder exists, if it doesn't, then create it
    # if not exists(plots_folder):
    #     makedirs(plots_folder)

    try:
        date_time = datetime.datetime.strptime(sys.argv[2], "%Y-%m-%d")
        date = date_time.date()
    except:
        print("Input string is not a correctly formatted date of form ('%Y-%m-%d')- %s" % (sys.argv[2]))
        raise SystemExit

    print(file_name)

    all_transfers = parse_xfer_data_logs.parse_logs(file_name)

    transfers = get_transfers_on_day(all_transfers, date)

    interval_length = datetime.timedelta(minutes=1)

    # date_time = datetime.datetime(year=date.year, month=date.month, day=date.day)
    plot_date_range = (date_time, date_time + datetime.timedelta(days=1))

    heuristic_tup = 'baseline', simulate.baseline_heuristic

    simulate.prepare_simulation(transfers, interval_length, date, plot_date_range, file_name, heuristic_tup)


# returns a list of all the transfers on the given day
def get_transfers_on_day(transfers, date):
    transfers_on_day = []

    for transfer in transfers:
        # if transfer.start_time.date() == date or transfer.end_time.date() == date or
        if transfer.requested_start_time.date() <= date <= transfer.requested_end_time.date():
            transfers_on_day.append(transfer)

    # sort transfers by start_time
    from operator import attrgetter
    transfers_on_day = sorted(transfers_on_day, key=attrgetter('requested_start_time'))

    return transfers_on_day


def print_transfers_on_day(all_transfers, date_to_print):
    transfers_on_day = get_transfers_on_day(all_transfers, date_to_print)

    # sort by transfer rate descending
    from operator import attrgetter
    transfers = sorted(transfers_on_day, key=attrgetter('requested_rate'), reverse=True)

    print("%s - %d transfers" % (date_to_print, len(transfers)))
    for idx, transfer in enumerate(transfers):
        print('{}: {}'.format(idx, transfer))

        # print("%d: %s - %s; Rate: %d; Bytes: %d" % (idx, transfer['request_time'], transfer['complete_time'],
        #                                             transfer['rate'], transfer['bytes']))


if __name__ == "__main__":
    main()
