import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def find_variants(event_log):
    # Extract the variants from an event log

    from pm4py.algo.filtering.log.variants import variants_filter
    variants = variants_filter.get_variants(event_log)
    print(len(variants))

    # Count the cases per variant
    from pm4py.statistics.traces.generic.log import case_statistics
    variants_count = case_statistics.get_variant_statistics(event_log)
    variants_count = sorted(variants_count, key=lambda x: x['count'], reverse=True)

    variants_df = pd.DataFrame.from_records(variants_count)

    pd.set_option('display.max_rows', None)
    pd.options.display.max_colwidth = 1000

    print(variants_df.head(100))
    return variants_df



def String2List(string,separ):
    li = list(string.split(separ))
    return li

def print_variant(variants_df):
    # Prepare the dimensions to plot
    # Put variants and counts into lists of prefered length

    variant = variants_df.head(100).index
    frequency = variants_df['count'].head(100)
    print(frequency)

    # Plot the histogram of the frequencies

    fig = plt.figure(figsize=(15, 5))

    # creating the bar plot
    plt.bar(variant, frequency, color='orange',
            width=0.4)

    plt.axis([0, 100, 0, max(variants_df['count'])])
    plt.xlabel("variants sorted by frequency")
    plt.ylabel("frequency")
    plt.title("bar chart of variants frequency")
    plt.show()

def plot_PDF_and_CDF(variants_df):
    df_in_focus = variants_df.head(100)
    data = df_in_focus['count']

    count, bins_count = np.histogram(data, bins=100) #50

    pdf = count / sum(count)

    cdf = np.cumsum(pdf)

    # plotting PDF and CDF
    plt.plot(bins_count[1:], pdf, color="red", label="PDF")
    plt.plot(bins_count[1:], cdf, label="CDF")
    plt.legend()
    plt.show()

def filter_on_variants_pareto(event_log, percentage):
    from pm4py.algo.filtering.log.variants import variants_filter

    filtered_log = variants_filter.filter_log_variants_percentage(event_log, percentage=percentage)

    return filtered_log

def analyze_variants(event_log, percentage):
    variants_df = find_variants(event_log)
    print_variant(variants_df)
    plot_PDF_and_CDF(variants_df)
    filtered_log = filter_on_variants_pareto(event_log, percentage)
    print(filtered_log)
    return filtered_log
