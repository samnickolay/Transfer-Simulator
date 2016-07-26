import datetime
import matplotlib as mpl
import matplotlib.pyplot as plt
import os


def plot_intervals(filename, title, intervals_list):

    fig, ax = plt.subplots()

    bytes_in_MiB = 1024 * 1024

    y_label = 'Network Demand (MiB/S)'
    x_label = 'Time of Day'
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(title)

    colors = ['cornflowerblue', 'red', 'green']
    line_counter = 0

    for idx, (label, intervals) in enumerate(intervals_list):
        x = [interval.start_time for interval in intervals]
        y = [interval.network_load()/bytes_in_MiB for interval in intervals]

        plt.plot(x, y, color=colors[idx], linestyle='-', linewidth=0.5, label=label)

    if len(intervals_list) > 1:
        plt.legend(loc=9, bbox_to_anchor=(0.5, -0.1), ncol=len(intervals_list), borderaxespad=1.7)

    # get the first set of intervals from the dictionary
    intervals = intervals_list[0][1]

    tick_freq = 3   # 1 x =_tick every 3 hours
    num_ticks = int((intervals[-1].end_time - intervals[0].start_time).total_seconds() / (tick_freq * 3600))
    x_tick_list = [intervals[0].start_time + datetime.timedelta(hours=i*tick_freq) for i in range(num_ticks+1)]
    ax.set_xticks(x_tick_list)

    ax.xaxis.set_minor_locator(mpl.dates.HourLocator())
    ax.xaxis.set_major_formatter(mpl.dates.DateFormatter('%H:%M'))

    date_min = intervals[0].start_time - datetime.timedelta(hours=1)
    date_max = intervals[-1].end_time + datetime.timedelta(hours=1)
    ax.set_xlim(date_min, date_max)

    fig.autofmt_xdate()

    print("\nSaving plot to %s" % filename)

    verify_filename(filename)

    plt.savefig(filename, dpi=250)

    plt.close(fig)


# make sure the plot filename is valid (i.e. all of the parent directories exist)
def verify_filename(filename):
    for idx, cur_char in enumerate(filename):
        if cur_char is '/':
            cur_dir_path = filename[:idx]

            if not os.path.exists(cur_dir_path):
                os.makedirs(cur_dir_path)



