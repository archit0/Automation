import pandas as pd
import numpy as np
import psycopg2
import time
import datetime
import sys

print("Execution start")
# database credentials
database1 = "opsfeaturizer"
database2 = "alchemy"
db_user = "slickdeal_dev_user"
db_password = "UfHK3bnMy78siAie"
db_host = "slickdeals-alchemy.cyili6nyniah.us-west-2.redshift.amazonaws.com"
db_port = "5439"

conn1 = psycopg2.connect(
    dbname=database1,
    user=db_user,
    password=db_password,
    host=db_host,
    port=db_port
)

conn2 = psycopg2.connect(
    dbname=database2,
    user=db_user,
    password=db_password,
    host=db_host,
    port=db_port
)
user_df = pd.read_sql_query('select id from segment', con=conn2)
value_df = pd.read_sql_query('select * from segment_attribute_time_value where segment_attribute_id in (18,19) and attribute_granularity_id = 3;', con=conn2)
attr_df = pd.read_sql_query('select id, name from segment_attribute where id in (18,19);', con=conn2)

final_df = pd.merge(attr_df, value_df, how='left', left_on='id', right_on='segment_attribute_id')
final_df = pd.pivot_table(final_df, columns=['name'], index=['segment_id', 'date_index', 'year', 'month'], values=['value'], aggfunc=max).reset_index()
final_df.columns =['_'.join(col).rstrip('_') for col in final_df.columns.values]
df_final= final_df.rename(columns={'year':'y', 'month':'m','value_content_consumed_number_of_subscriptions':'total_number_of_alerts', 'value_average_alerts_per_user':'average_alerts_per_user'})

df1=pd.merge(df_final, user_df, how='left', left_on='segment_id', right_on='id')
df1 = df1[['date_index', 'y', 'm', 'total_number_of_alerts', 'average_alerts_per_user', 'segment_id']]
df1['average_alerts_per_user'] = df1['average_alerts_per_user'].astype(float)
df1['average_alerts_per_user'] = df1['average_alerts_per_user'].astype(int)
df1.sort_values(['segment_id', 'y', 'm', 'date_index'], ascending = True, inplace = True)
sql1 = '''SELECT date_index, y, m, total_number_of_alerts, average_alerts_per_user, segment_id from audience_monthly_content_consumed_alerts_trend;'''

df2 = pd.read_sql_query(sql1, con=conn1)
df2.sort_values(['segment_id', 'y', 'm', 'date_index'], ascending = True, inplace = True)
print ('Retrieved featurized query through SQL...')

print("comperision started")
unique = 'segment_id'  # storing unique column name in variable as index for comparison of test result later...
temp_list = []  # this list is used to store temporary values
source_1 = 'Canonical Output'
source_2 = 'Ops Output'

df1.sort_index(axis=1, inplace=True)  # Sorting the data frame on column
df1.sort_values(unique, ascending=True, inplace=True)  # Sorting the data frame on row
df1.fillna('', inplace=True)  # convert NULL values to blank - this is needed for comparison
df1 = df1.applymap(str)  # convert all values to string - this is needed for comparison

# Refer to comments for 1st dataframe above
df2.sort_index(axis=1, inplace=True)
df2.sort_values(unique, ascending = True, inplace=True)
df2.fillna('', inplace=True)
df2 = df2.applymap(str)

# storing all unique values in base column
list_1 = df1[unique].tolist()
list_2 = df2[unique].tolist()

# storing header to lists
df1_header = list(df1)
df2_header = list(df2)

# identifying unique headers
df1_unique_header = set(df1_header) - set(df2_header)
df2_unique_header = set(df2_header) - set(df1_header)

print('There are ', len(df1_unique_header), 'unique columns in', source_1, ':', list(df1_unique_header))
print('There are ', len(df2_unique_header), 'unique columns in', source_2, ':', list(df2_unique_header))

# Removing unique columns from dataframe so that same column values can be compared...
if (len(df1_unique_header) > 0):
    print('removing duplicate columns in ', source_1)
    df1.drop(list(df1_unique_header), axis=1, inplace=True)
    df1_header = list(df1)
if (len(df2_unique_header) > 0):
    print('removing duplicate columns in ', source_2)
    df2.drop(list(df2_unique_header), axis=1, inplace=True)
    df2_header = list(df2)


df_csv_summary = pd.DataFrame(columns=['Item', source_1, source_2])

df_csv_summary['Item'] = ['No: of Columns', 'No: of Rows', 'No: of Rows with matching ' + unique,\
                          'Unique Cols (Major Issue)', ' No: of Unique Rows (Major Issue)', \
                          'Duplicate Rows', 'Different rows with Duplicate ' + unique + 's',\
                          'Columns after optimizing', 'Rows after optimizing',\
                          'Comparisons: ', '', 'Failed Columns']


df_csv_summary.at[0, source_1] = len(df1.columns)
df_csv_summary.at[0, source_2] = len(df2.columns)

df_csv_summary.at[1, source_1] = len(list_1)
df_csv_summary.at[1, source_2] = len(list_2)

df_csv_summary.at[5, source_1] = 'Unverified'
df_csv_summary.at[5, source_2] = 'Unverified'

df_csv_summary.at[6, source_1] = 'Unverifed'
df_csv_summary.at[6, source_2] = 'Unverified'

df_csv_summary.at[9, source_1] = 'Pass: '
df_csv_summary.at[9, source_2] = 'Fail: '

df_csv_summary.at[10, source_1] = ''
df_csv_summary.at[10, source_2] = ''

df_csv_summary.at[11, source_1] = 'Total Rows'
df_csv_summary.at[11, source_2] = 'Mismatch #'



df1_diff = df1[~df1[unique].isin(list_2)]
df2_diff = df2[~df2[unique].isin(list_1)]


# Identifying unique items in source and destination
df_unique_ids = pd.DataFrame(index=np.arange(max(len(df1_diff), len(df2_diff))), columns=[source_1, source_2])
df_unique_ids[source_1] = pd.Series(df1_diff[unique].tolist())
df_unique_ids[source_2] = pd.Series(df2_diff[unique].tolist())
df_unique_ids.insert(loc=0, column='Sl No', value=df_unique_ids.index+1)   

# Identifying duplicate columns in source and destination
df_unique_cols = pd.DataFrame(index=np.arange(max(len(df1_unique_header), len(df2_unique_header))), columns=[source_1, source_2])
df_unique_cols[source_1] = pd.Series(list(df1_unique_header))
df_unique_cols[source_2] = pd.Series(list(df2_unique_header))
df_unique_cols.insert(loc=0, column='Sl No', value=df_unique_cols.index + 1)

df_csv_summary.at[3, source_1] = len(df1_unique_header)
df_csv_summary.at[3, source_2] = len(df2_unique_header)

df_csv_summary.at[4, source_1] = len(pd.Series(df1_diff[unique].tolist()))
df_csv_summary.at[4, source_2] = len(pd.Series(df2_diff[unique].tolist()))


# Removing unique rows in each data frame
df1 = df1[df1[unique].isin(list_2)]
df2 = df2[df2[unique].isin(list_1)]

df_csv_summary.at[2, source_1] = len(df1)
df_csv_summary.at[2, source_2] = len(df2)

dup_counter = 0

if len(df1) != len(df2):  # remove duplicate rows if number of rows in both data frames are not the same
    print('Removing duplicate rows...')
    row_dup = df1.duplicated(keep='last')
    df_dup = df1[row_dup]
    df_dup.to_csv('Dup_All_Rows_'+source_1+'.csv', index=False)
    df_csv_summary.at[5, source_1] = len(df_dup)
    print('Removed ', len(df_dup), 'duplicate rows from source...')
    if len(df_dup) > 0:
        dup_counter += len(df_dup)

    row_dup = df2.duplicated(keep='last')
    df_dup = df2[row_dup]
    df_dup.to_csv('Dup_All_Rows_'+source_2+'.csv', index=False)
    df_csv_summary.at[5, source_2] = len(df_dup)
    print('Removed ', len(df_dup), 'duplicate rows from destination...')
    if len(df_dup) > 0:
        dup_counter += len(df_dup)

    df1.drop_duplicates(keep='last', inplace=True)
    df2.drop_duplicates(keep='last', inplace=True)

if len(df1) != len(df2):  # remove rows having duplicate ID if number of rows in both data frames are still not the same
    print('Removing rows containing different values but same ', unique,'s...')
    row_dup = df1.duplicated(subset=unique, keep='last')
    df_dup = df1[row_dup]
    df_dup.to_csv('Dup_'+unique+'_'+source_1+'.csv', index=False)
    df_csv_summary.at[6, source_1] = len(df_dup)
    print ('Removed ', len(df_dup), 'rows with duplicate', unique, 'from source...')
    if len(df_dup) > 0:
        dup_counter += len(df_dup)

    row_dup = df2.duplicated(subset=unique, keep='last')
    df_dup = df2[row_dup]
    df_dup.to_csv('Dup_'+unique+'_'+source_2+'.csv', index=False)
    df_csv_summary.at[6, source_2] = len(df_dup)
    print ('Removed ', len(df_dup), 'rows with duplicate', unique, 'from destination...')
    if len(df_dup) > 0:
        dup_counter += len(df_dup)


# df_dup.drop(df_dup.index, inplace=True)

    df1.drop_duplicates(subset=unique, keep='last', inplace=True)
    df2.drop_duplicates(subset=unique, keep='last', inplace=True)

df1.reset_index(drop=True, inplace=True)
df2.reset_index(drop=True, inplace=True)


# df1.to_csv('1_updated_sorted.csv', index=False)
# df2.to_csv('2_updated_sorted.csv', index=False)

df_csv_summary.at[7, source_1] = len(df1.columns)
df_csv_summary.at[7, source_2] = len(df2.columns)

df_csv_summary.at[8, source_1] = len(df1)
df_csv_summary.at[8, source_2] = len(df2)

time_loop_start = time.time()
diff_counter = 0
difference_list = []
columns = ['Col No', 'Col Name', 'Row No', source_1 + ' Val', source_2 + ' Val', unique]


if df1.equals(df2):
        print ('Both comparable datasets are the same')
else:  # Looping through datasets to find and list differences
    for i in range (0, len(df1.columns)):
        df_to_db3_column_data=df1[list(df1)[i]].tolist()
        df_latest_sql_column_data=df2[list(df2)[i]].tolist()
        for j in range (0,len(df_to_db3_column_data)):
            if df_to_db3_column_data[j] != df_latest_sql_column_data[j]:
                difference_list.append([i + 1, df1_header[i], j + 1, df_to_db3_column_data[j], df_latest_sql_column_data[j], df1.loc[j, unique]])
                diff_counter += 1

time_loop_end = time.time()

df_diff = pd.DataFrame(difference_list,columns=columns)

if diff_counter == 0:
    if len(df_unique_ids) == 0 and dup_counter == 0:
        print("Both files are identical")
    else:
        print ('Unique rows are identical, but there are some rows that are either missing or duplicated..')
else:
    df_diff.drop(df_diff[df_diff[source_1 + ' Val'] == df_diff[source_2 + ' Val']].index, inplace=True)
    df_diff.reset_index(drop=True, inplace=True)
    
    df_diff.insert(loc=0, column='Sl No', value=df_diff.index+1)   
    print ('Both data is NOT same, there are', diff_counter, 'differences apart from', \
           (len(pd.Series(df1_diff[unique].tolist())) + len(pd.Series(df2_diff[unique].tolist()))), 'rows with different', unique)

df_csv_summary.at[9, 'Item'] = 'Comparisons: ' + str(len(df1.columns) * len(df1))
df_csv_summary.at[9, source_1] = 'Pass: ' + str((len(df1.columns) * len(df1)) - diff_counter)
df_csv_summary.at[9, source_2] = 'Fail: ' + str(diff_counter)

print('Total No of comparisons done is', len(df1.columns) * len(df1), 'in', round((time_loop_end - time_loop_start), 4), 'seconds. Summary below...\n')

failed_cols = pd.Series(df_diff['Col Name'].unique()).tolist()

for i in failed_cols:
    temp_list.append(i)
    temp_list.append(len(df1))
    temp_list.append((df_diff['Col Name'] == i).sum())
    if temp_list[1] == temp_list[2]:
        temp_list[2] = 'All'
    df_csv_summary.loc[len(df_csv_summary)] = temp_list
    temp_list.clear()

print(df_csv_summary.head(10))

result_file = 'A360M_content_consumed_alerts_trend.xlsx'
with pd.ExcelWriter(result_file) as writer:
    df_csv_summary.to_excel(writer, 'Summary', index = False)
    df_unique_ids.to_excel(writer, 'U_'+unique+'_list', index = False)
    df_unique_cols.to_excel(writer, 'Unique_Columns', index=False)
    df_diff.to_excel(writer, 'Mismatch_in_sorted_xlsx', index = False)