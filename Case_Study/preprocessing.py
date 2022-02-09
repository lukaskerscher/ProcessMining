import pandas as pd
import pm4py
from pm4py.objects.log.util import dataframe_utils
from pm4py.objects.conversion.log import converter as log_converter

log_csv = None

parameters = None
event_log = None

df_3waymatch_i_a_gr = None
df_3waymatch_i_b_gr = None
df_2waymatch = None
df_consignment = None

datasets_name = None
datasets = None
datasets_filtered_name = None
datasets_filtered = None
eventlogs_name = None
eventlogs = None


import numpy as np

def load_data():
    # load event log file in csv
    log_csv = pd.read_csv('BPI_Challenge_2019.csv', encoding='cp1252', sep=',')

    log_csv = dataframe_utils.convert_timestamp_columns_in_df(log_csv)

    log_csv = log_csv.sort_values('event time:timestamp')

    # rename columns
    log_csv.rename(columns={
                            'case Spend area text': 'case:spendArea:text',
                            'case Company': 'case:company',
                            'case Document Type': 'case:documentType',
                            'case Sub spend area text': 'case:subSpendArea:text',
                            'case Purchasing Document': 'case:purchasingDocument',
                            'case Purch. Doc. Category name': 'case:purchasingDocumentCategory:name',
                            'case Vendor': 'case:vendor',
                            'case Item Type': 'case:item:type',
                            'case Item Category': 'case:item:category',
                            'case Spend classification text': 'case:spendClassification:text',
                            'case Name': 'case:name',
                            'case Source': 'case:source',
                            'case GR-Based Inv. Verif.': 'case:GR-BasedInv.Verif.',
                            'case Item': 'case:item',
                            'case concept:name': 'case:concept:name',
                            'case Goods Receipt': 'case:goodsReceipt',
                            'event User': 'event:user',
                            'event org:resource': 'org:resource',
                            'event concept:name': 'concept:name',
                            'event Cumulative net worth (EUR)': 'event:cumulativeNetWorth(EUR)',
                            'event time:timestamp': 'time:timestamp'}, inplace=True)
    #print(log_csv)
    print("Data loaded.\nColumns renamed.")
    return log_csv

def generate_overall_event_log(log_csv):
    # Generate event log

    from pm4py.objects.conversion.log import converter as log_converter

    global parameters

    parameters = {log_converter.Variants.TO_EVENT_LOG.value.Parameters.CASE_ID_KEY: 'case:concept:name'}
    event_log = log_converter.apply(log_csv, parameters=parameters, variant=log_converter.Variants.TO_EVENT_LOG)

    print('Event Log generated.')
    return event_log

def split_dataset(log_csv):
    # Create seperated dataFrames for the different categories
    global df_3waymatch_i_a_gr
    global df_3waymatch_i_b_gr
    global df_2waymatch
    global df_consignment

    df_3waymatch_i_a_gr = log_csv.loc[(log_csv['case:item:category'] == '3-way match, invoice after GR')]
    df_3waymatch_i_b_gr = log_csv.loc[(log_csv['case:item:category'] == '3-way match, invoice before GR')]
    df_2waymatch = log_csv.loc[(log_csv['case:item:category'] == '2-way match')]
    df_consignment = log_csv.loc[(log_csv['case:item:category'] == 'Consignment')]

    print('Dataset split.')

def check_if_split_was_sucessful():
    # double check if splitting up dataset was sucessful

    global df_3waymatch_i_a_gr
    global df_3waymatch_i_b_gr
    global df_2waymatch
    global df_consignment

    print('check_if_split_was_sucessful')

    # For df_3waymatch_i_a_gr
    print('df_3waymatch_i_a_gr')
    val_before_check = df_3waymatch_i_a_gr['case:concept:name'].count()

    df_3waymatch_i_a_gr = df_3waymatch_i_a_gr[
        ((df_3waymatch_i_a_gr['case:GR-BasedInv.Verif.'] == True) & (df_3waymatch_i_a_gr['case:goodsReceipt'] == True))]

    val_after_check = df_3waymatch_i_a_gr['case:concept:name'].count()

    print("Before: " + str(val_before_check))
    print("After: " + str(val_after_check))

    # For df_3waymatch_i_b_gr
    print('\ndf_3waymatch_i_b_gr')
    val_before_check = df_3waymatch_i_b_gr['case:concept:name'].count()

    df_3waymatch_i_b_gr = df_3waymatch_i_b_gr[((df_3waymatch_i_b_gr['case:GR-BasedInv.Verif.'] == False) & (
                df_3waymatch_i_b_gr['case:goodsReceipt'] == True))]

    val_after_check = df_3waymatch_i_b_gr['case:concept:name'].count()

    print("Before: " + str(val_before_check))
    print("After: " + str(val_after_check))

    # For df_2waymatch
    print('\ndf_2waymatch')
    val_before_check = df_2waymatch['case:concept:name'].count()

    df_2waymatch = df_2waymatch[
        ((df_2waymatch['case:GR-BasedInv.Verif.'] == False) & (df_2waymatch['case:goodsReceipt'] == False))]

    val_after_check = df_2waymatch['case:concept:name'].count()

    print("Before: " + str(val_before_check))
    print("After: " + str(val_after_check))

    # For df_consignment
    print('\ndf_consignment')
    val_before_check = df_consignment['case:concept:name'].count()

    df_consignment = df_consignment[
        ((df_consignment['case:GR-BasedInv.Verif.'] == False) & (df_consignment['case:goodsReceipt'] == True))]

    val_after_check = df_consignment['case:concept:name'].count()

    print("Before: " + str(val_before_check))
    print("After: " + str(val_after_check))

    print('Sucess check executed.')

def create_lists():
    # Create lists of dataframes in order to automate iterations

    global datasets_name
    global datasets
    global datasets_filtered_name
    global datasets_filtered
    global eventlogs_name
    global eventlogs

    datasets_name = ['df_3waymatch_i_a_gr', 'df_3waymatch_i_b_gr', 'df_2waymatch', 'df_consignment']
    datasets = [df_3waymatch_i_a_gr, df_3waymatch_i_b_gr, df_2waymatch, df_consignment]

    datasets_filtered_name = ['df_filtered_3waymatch_i_a_gr', 'df_filtered_3waymatch_i_b_gr', 'df_filtered_2waymatch',  'df_filtered_consignment']
    df_filtered_3waymatch_i_a_gr, df_filtered_3waymatch_i_b_gr, df_filtered_2waymatch, df_filtered_consignment = df_3waymatch_i_a_gr, df_3waymatch_i_b_gr, df_2waymatch, df_consignment
    datasets_filtered = [df_filtered_3waymatch_i_a_gr, df_filtered_3waymatch_i_b_gr, df_filtered_2waymatch, df_filtered_consignment]

    # Create lists of event logs in order to automate iterations
    eventlogs_name = ['EVENTLOG_3waymatch_i_a_gr', 'EVENTLOG_3waymatch_i_b_gr', 'EVENTLOG_2waymatch',
                      'EVENTLOG_consignment']

    eventlogs = [
        log_converter.apply(df_3waymatch_i_a_gr, parameters=parameters, variant=log_converter.Variants.TO_EVENT_LOG),
        log_converter.apply(df_3waymatch_i_b_gr, parameters=parameters, variant=log_converter.Variants.TO_EVENT_LOG),
        log_converter.apply(df_2waymatch, parameters=parameters, variant=log_converter.Variants.TO_EVENT_LOG),
        log_converter.apply(df_consignment, parameters=parameters, variant=log_converter.Variants.TO_EVENT_LOG)]

    print('Lists created.')


def filter_on_timeframe():
    # Filtering on timeframe

    global datasets_filtered
    global datasets

    from pm4py.algo.filtering.pandas.timestamp import timestamp_filter

    for count, elem in enumerate(datasets_filtered):
        datasets_filtered[count] = timestamp_filter.filter_traces_contained(datasets_filtered[count], "2018-01-01 00:00:00",
                                                                            "2019-01-28 23:59:59",
                                                                            parameters={
                                                                                timestamp_filter.Parameters.CASE_ID_KEY: "case:concept:name",
                                                                                timestamp_filter.Parameters.TIMESTAMP_KEY: "time:timestamp"})

    # Check filtered out values
    for count, elem in enumerate(datasets):
        print('\n' + datasets_name[count])
        print('Cases:')
        print('Before: ' + str(elem['case:concept:name'].nunique()))
        print('After:  ' + str(datasets_filtered[count]['case:concept:name'].nunique()))
        print('Activities:')
        print('Before: ' + str(elem['concept:name'].count()))
        print('After:  ' + str(datasets_filtered[count]['concept:name'].count()))

def update_eventlog():
    global eventlogs
    global datasets_filtered

    for count, elem in enumerate(eventlogs):
        eventlogs[count] = log_converter.apply(datasets_filtered[count], parameters=parameters,
                                   variant=log_converter.Variants.TO_EVENT_LOG)

def update_dataframe():
    global eventlogs
    global datasets_filtered

    for count, elem in enumerate(datasets_filtered):
        datasets_filtered[count] = log_converter.apply(eventlogs[count], parameters=parameters,
                                   variant=log_converter.Variants.TO_DATA_FRAME)

def conmpliance_checking():
    # conmpliance checking for the different categories

    global eventlogs

    from pm4py.algo.filtering.log.attributes import attributes_filter
    # Conformance checking for df_3waymatch_i_a_gr
    eventlogs[0] = attributes_filter.apply(eventlogs[0],
                                           ["Record Goods Receipt", "Clear Invoice", "Record Invoice Receipt"],
                                           parameters={attributes_filter.Parameters.ATTRIBUTE_KEY: "concept:name",
                                                       attributes_filter.Parameters.POSITIVE: True})

    # Conformance checking for df_3waymatch_i_b_gr
    eventlogs[1] = attributes_filter.apply(eventlogs[1],
                                           ["Record Goods Receipt", "Clear Invoice", "Record Invoice Receipt"],
                                           parameters={attributes_filter.Parameters.ATTRIBUTE_KEY: "concept:name",
                                                       attributes_filter.Parameters.POSITIVE: True})

    # Conformance checking for df_2waymatch
    eventlogs[2] = attributes_filter.apply(eventlogs[2], ["Clear Invoice", "Record Invoice Receipt"],
                                           parameters={attributes_filter.Parameters.ATTRIBUTE_KEY: "concept:name",
                                                       attributes_filter.Parameters.POSITIVE: True})

    # Conformance checking for df_consignment
    eventlogs[3] = attributes_filter.apply(eventlogs[3], ["Record Goods Receipt"],
                                           parameters={attributes_filter.Parameters.ATTRIBUTE_KEY: "concept:name",
                                                       attributes_filter.Parameters.POSITIVE: True})


def filter_incomplete_cases():
    # df_3waymatch_i_a_gr
    print("df_3waymatch_i_a_gr")
    global datasets_filtered

    temp_dataset = datasets_filtered[0][['case:concept:name', 'concept:name']]
    temp_dataset['count'] = 1

    pivot_table = pd.pivot_table(temp_dataset, values='count', index=['case:concept:name'],
                                 columns='concept:name', aggfunc=np.size).reset_index()

    newtable = pivot_table[pivot_table["Record Invoice Receipt"] == pivot_table["Record Goods Receipt"]]

    # pivot_table
    newtable.set_index('case:concept:name')
    newtable = newtable[['case:concept:name']]
    list = newtable['case:concept:name'].tolist()

    datasets_filtered[0] = datasets_filtered[0][temp_dataset['case:concept:name'].isin(list)]

    print(datasets_filtered[0]['case:concept:name'].nunique())

    # df_3waymatch_i_b_gr
    print("df_3waymatch_i_b_gr")

    temp_dataset = datasets_filtered[1][['case:concept:name', 'concept:name']]
    temp_dataset['count'] = 1

    pivot_table = pd.pivot_table(temp_dataset, values='count', index=['case:concept:name'],
                                 columns='concept:name', aggfunc=np.size).reset_index()

    newtable = pivot_table[pivot_table["Record Invoice Receipt"] == pivot_table["Record Goods Receipt"]]

    # pivot_table
    newtable.set_index('case:concept:name')
    newtable = newtable[['case:concept:name']]
    list = newtable['case:concept:name'].tolist()

    datasets_filtered[1] = datasets_filtered[1][temp_dataset['case:concept:name'].isin(list)]
    print(datasets_filtered[1]['case:concept:name'].nunique())


# Currently not in use
def filter_on_end_activities():
    # Filter on End activities
    from pm4py.algo.filtering.pandas.end_activities import end_activities_filter

    # df_filtered_3waymatch_i_a_gr
    # df_filtered_3waymatch_i_b_gr
    # df_filtered_2waymatch

    for count in range(3):
        print(datasets_filtered_name[count] + '\nBefore:')
        print('Cases: ' + str(datasets_filtered[count]['case:concept:name'].nunique()))
        print('Activities: ' + str(datasets_filtered[count]['concept:name'].count()))

        end_activities = end_activities_filter.get_end_activities(datasets_filtered[count])
        print(end_activities)

        datasets_filtered[count] = end_activities_filter.apply(datasets_filtered[count], ["Clear Invoice"],
                                                               parameters={
                                                                   end_activities_filter.Parameters.CASE_ID_KEY: "case:concept:name",
                                                                   end_activities_filter.Parameters.ACTIVITY_KEY: "concept:name"})
        datasets_filtered[count]

        print('After:')
        print('Cases: ' + str(datasets_filtered[count]['case:concept:name'].nunique()))
        print('Activities: ' + str(datasets_filtered[count]['concept:name'].count()) + '\n')

    # df_filtered_consignment
    print('df_filtered_consignment\nBefore:')
    print('Cases: ' + str(datasets_filtered[3]['case:concept:name'].nunique()))
    print('Activities: ' + str(datasets_filtered[3]['concept:name'].count()))

    end_activities = end_activities_filter.get_end_activities(datasets_filtered[3])
    print(end_activities)

    df_filtered_consignment = end_activities_filter.apply(datasets_filtered[3], ["Record Goods Receipt"],
                                                          parameters={
                                                              end_activities_filter.Parameters.CASE_ID_KEY: "case:concept:name",
                                                              end_activities_filter.Parameters.ACTIVITY_KEY: "concept:name"})
    df_filtered_consignment

    print('After:')
    print('Cases: ' + str(df_filtered_consignment['case:concept:name'].nunique()))
    print('Activities: ' + str(df_filtered_consignment['concept:name'].count()))


def check_invoice_after_GR(df):
    result_list = []
    df_cases = df['case:concept:name'].unique().tolist()

    #print(df_cases)
    lenth = len(df_cases)
    counter_app = 0

    for case in df_cases:
        temp_df = df.loc[(df['case:concept:name'] == case)]
        temp_df = temp_df.sort_values('time:timestamp')

        df_activities = temp_df['concept:name'].tolist()
        for elem in df_activities:
            if(elem=='Record Goods Receipt'):
                result_list.append(case)
                counter_app = counter_app + 1
                print('append: ' + str(case) + ' - ' + str(counter_app) + ' / ' + str(lenth))
                break
            elif(elem=='Record Invoice Receipt'):
                counter_app = counter_app + 1
                print('not append: ' + str(case) + ' - ' + str(counter_app) + ' / ' + str(lenth))
                break

    result_df = pd.DataFrame(result_list, columns=['case:concept:name'])

    df = df.merge(result_df, how='inner', on='case:concept:name')

    return df

def check_invoice_before_GR(df):
    result_list = []
    df_cases = df['case:concept:name'].unique().tolist()

    lenth = len(df_cases)
    counter_app = 0

    for case in df_cases:
        temp_df = df.loc[(df['case:concept:name'] == case)]
        temp_df = temp_df.sort_values('time:timestamp')

        df_activities = temp_df['concept:name'].tolist()

        for elem in df_activities:
            if(elem=='Record Invoice Receipt'):
                result_list.append(case)
                counter_app = counter_app + 1
                print('append: ' + str(case) + ' - ' + str(counter_app) + ' / ' + str(lenth))
                break
            elif(elem=='Record Goods Receipt'):
                counter_app = counter_app + 1
                print('not append: ' + str(case) + ' - ' + str(counter_app) + ' / ' + str(lenth))
                break


    result_df = pd.DataFrame(result_list, columns=['case:concept:name'])

    df = df.merge(result_df, how='inner', on='case:concept:name')

    return df

def add_additional_columns():
    global datasets_filtered

    for elem in datasets_filtered:
        elem['user_type'] = elem['org:resource'].apply(
            lambda x: 'user' if ("user" in x) else 'batch' if ("batch" in x) else "NONE")

    print('Added additional columns.')



######## SCRIPT #########

def preprocess_data():

    global datasets_filtered

    log_csv = load_data()
    event_log = generate_overall_event_log(log_csv)
    split_dataset(log_csv)
    check_if_split_was_sucessful()
    create_lists()
    add_additional_columns();

    datasets_filtered[0].to_csv('RAW_3waymatch_i_a_gr.csv', index=False)
    datasets_filtered[1].to_csv('RAW_3waymatch_i_b_gr.csv', index=False)
    datasets_filtered[2].to_csv('RAW_2waymatch.csv', index=False)
    datasets_filtered[3].to_csv('RAW_consignment.csv', index=False)


    print('Dataset split:')
    print(datasets_filtered[0]['case:concept:name'].nunique())
    print(datasets_filtered[1]['case:concept:name'].nunique())
    print(datasets_filtered[2]['case:concept:name'].nunique())
    print(datasets_filtered[3]['case:concept:name'].nunique())


    filter_on_timeframe()
    update_eventlog()
    conmpliance_checking()
    update_dataframe()

    print('conmpliance_checking:')
    print(datasets_filtered[0]['case:concept:name'].nunique())
    print(datasets_filtered[1]['case:concept:name'].nunique())
    print(datasets_filtered[2]['case:concept:name'].nunique())
    print(datasets_filtered[3]['case:concept:name'].nunique())


    filter_incomplete_cases()

    print('filter_incomplete_cases:')
    print(datasets_filtered[0]['case:concept:name'].nunique())
    print(datasets_filtered[1]['case:concept:name'].nunique())
    print(datasets_filtered[2]['case:concept:name'].nunique())
    print(datasets_filtered[3]['case:concept:name'].nunique())

    print('Final Results (after before/after filter):')

    #after
    datasets_filtered[0] = check_invoice_after_GR(datasets_filtered[0])

    #before
    datasets_filtered[1] = check_invoice_before_GR(datasets_filtered[1])

    print(datasets_filtered[0]['case:concept:name'].nunique())
    print(datasets_filtered[1]['case:concept:name'].nunique())
    print(datasets_filtered[2]['case:concept:name'].nunique())
    print(datasets_filtered[3]['case:concept:name'].nunique())

    #filter_on_end_activities()

    # safe data in csv files
    datasets_filtered[0].to_csv('df_3waymatch_i_a_gr.csv', index=False)
    datasets_filtered[1].to_csv('df_3waymatch_i_b_gr.csv', index=False)
    datasets_filtered[2].to_csv('df_2waymatch.csv', index=False)
    datasets_filtered[3].to_csv('df_consignment.csv', index=False)

#########################