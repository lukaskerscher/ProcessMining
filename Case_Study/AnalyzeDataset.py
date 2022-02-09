import pandas as pd
import matplotlib.pyplot as plt

from pm4py.objects.log.util import dataframe_utils

def goupby_attribute(df, attribute, aggregated_attribute):
    print('group by '+attribute)

    profile = df.groupby(attribute).agg(Cases=(aggregated_attribute, 'nunique'))
    profile = profile.sort_values('Cases')
    print(profile)

    # the top 5
    top5 = profile.tail(5)

    print(top5)

    # others
    print('len= ' + str(len(profile)))

    if len(profile) > 5:
        new_row = pd.DataFrame(sum(profile['Cases'].head(len(profile)-5)), index=['Others'], columns=['Cases'])
    else:
        new_row = pd.DataFrame(0, index=['Others'], columns=['Cases'])

    print(new_row)

    top5 = pd.concat([top5, new_row])

    print(top5)

    # Pie chart
    labels = top5.index
    sizes = top5['Cases']

    fig1, ax1 = plt.subplots()
    ax1.pie(sizes, autopct='%1.1f%%',
            shadow=False, startangle=90, pctdistance=1.1, labeldistance=1.2, radius=3)
    ax1.axis('equal')

    plt.legend(labels, loc='right')
    plt.show()

def analyze_general_metrics(df):
    print('analyze_general_metrics')

    from pm4py.algo.filtering.pandas.start_activities import start_activities_filter
    log_start = start_activities_filter.get_start_activities(df)

    from pm4py.algo.filtering.pandas.end_activities import end_activities_filter
    end_activities = end_activities_filter.get_end_activities(df)

    print('Distinct cases:              %s' % df['case:concept:name'].nunique())
    print('Overall activities:          %s' % df['concept:name'].count())
    print('Distinct activities:         %s' % df['concept:name'].nunique())
    print('Distinct Ressources:         %s      (with elements that have no ressource)' % df['org:resource'].nunique())
    print('Distinct Purchase Document:  %s' % df['case:purchasingDocument'].nunique())
    print('Distinct Vendors:            %s' % df['case:vendor'].nunique())
    print('Distinct Companies:          %s' % df['case:company'].nunique())
    print('Distinct Spend Areas:        %s' % df['case:spendArea:text'].nunique())
    print('Earliest Event:              %s' % df['time:timestamp'].min())
    print('Latest Event:                %s' % df['time:timestamp'].max())
    print('Start Activities:            %s' % len(log_start))
    print('End Activities:              %s' % len(end_activities))

def analyze_initial_dataset(df):

    goupby_attribute(df, 'user_type', 'eventID ')
    goupby_attribute(df, 'case:item:category', 'case:concept:name')
    goupby_attribute(df, 'case:company', 'case:concept:name')
    goupby_attribute(df, 'case:spendArea:text', 'case:concept:name')
    goupby_attribute(df, 'case:documentType', 'case:concept:name')
    goupby_attribute(df, 'case:purchasingDocumentCategory:name', 'case:concept:name')
    goupby_attribute(df, 'case:vendor', 'case:concept:name')
    goupby_attribute(df, 'case:item:type', 'case:concept:name')
    goupby_attribute(df, 'case:source', 'case:concept:name')
    goupby_attribute(df, 'org:resource', 'eventID ')
    goupby_attribute(df, 'concept:name', 'eventID ')

def analyze_by_case(log_csv):
    profile = log_csv.groupby('case:concept:name').agg(
        Activity=('concept:name', 'count'), \
        Resource=('org:resource', 'nunique'), \
        )

    profile = profile.sort_values('Activity', ascending=False)
    print(profile.max())
    print(profile.min())

    print(profile)

def load_data():
    # load event log file in csv
    log_csv = pd.read_csv('BPIChallenge2019CSV.zip', encoding='cp1252', sep=',')

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

##CALL FUNCTIONS HERE##
log_csv = load_data()
print(log_csv)
analyze_initial_dataset(log_csv)
analyze_by_case(log_csv)

RAW_df_3waymatch_i_a_gr = pd.read_csv('RAW_3waymatch_i_a_gr.csv', encoding='cp1252', sep=',')
RAW_df_3waymatch_i_b_gr = pd.read_csv('RAW_3waymatch_i_b_gr.csv', encoding='cp1252', sep=',')
RAW_df_2waymatch = pd.read_csv('RAW_2waymatch.csv', encoding='cp1252', sep=',')
RAW_df_consignment = pd.read_csv('RAW_consignment.csv', encoding='cp1252', sep=',')


analyze_initial_dataset(RAW_df_3waymatch_i_a_gr)
analyze_initial_dataset(RAW_df_3waymatch_i_b_gr)
analyze_initial_dataset(RAW_df_2waymatch)
analyze_initial_dataset(RAW_df_consignment)

#print('Overall')
#analyze_general_metrics(log_csv)
print('RAW_df_3waymatch_i_a_gr')
analyze_general_metrics(RAW_df_3waymatch_i_a_gr)
print('RAW_df_3waymatch_i_b_gr')
analyze_general_metrics(RAW_df_3waymatch_i_b_gr)
print('RAW_df_2waymatch')
analyze_general_metrics(RAW_df_2waymatch)
print('RAW_df_consignment')
analyze_general_metrics(RAW_df_consignment)
