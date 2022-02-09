import pandas as pd
from pm4py.objects.conversion.log import converter as log_converter

import preprocessing
import variants
import process_models

preprocessing.preprocess_data()

df_3waymatch_i_a_gr = pd.read_csv('df_3waymatch_i_a_gr.csv', encoding='cp1252', sep=',')
df_3waymatch_i_b_gr = pd.read_csv('df_3waymatch_i_b_gr.csv', encoding='cp1252', sep=',')
df_2waymatch = pd.read_csv('df_2waymatch.csv', encoding='cp1252', sep=',')
df_consignment = pd.read_csv('df_consignment.csv', encoding='cp1252', sep=',')

# Process df_3waymatch_i_b_gr
parameters = {log_converter.Variants.TO_EVENT_LOG.value.Parameters.CASE_ID_KEY: 'case:concept:name'}
EVENTLOG_3waymatch_i_b_gr = log_converter.apply(df_3waymatch_i_b_gr, parameters=parameters,
                                                    variant=log_converter.Variants.TO_EVENT_LOG)

percentage = 0.2
filtered_log = variants.analyze_variants(EVENTLOG_3waymatch_i_b_gr, percentage)

process_models.generate_process_models(filtered_log)

print('Done')