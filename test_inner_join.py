import pandas as pd

# * Test script to check Flow Join behavior *

# Input files
file_R = './cpp/bin/R_6.txt'
file_S = './cpp/bin/S_zipf_123_1p3_6_10.txt'


def inner_join_files(fileR, fileS, output_file):
    # Load data: txt
    df_R = pd.read_csv(fileR, sep=' ', header=None, names=['key', 'index_R'])
    df_S = pd.read_csv(fileS, sep=' ', header=None, names=['key', 'index_S'])

    # Convert R to a hash table for fast lookup
    # TODO: R always the smaller one?
    hash_table = {}
    for _, row in df_R.iterrows():
        if row['key'] not in hash_table:
            hash_table[row['key']] = []
        hash_table[row['key']].append(row['index_R'])

    # Join by iterating through S using the hash table
    joined_data = []
    for _, row in df_S.iterrows():
        if row['key'] in hash_table:
            for value_R in hash_table[row['key']]:
                joined_data.append([row['key'], value_R, row['index_S']])

    joined_df = pd.DataFrame(joined_data, columns=['key', 'index_R', 'index_S'])

    # Save as .txt
    joined_df.to_csv(output_file, sep=' ', index=False, header=True)

    return joined_df


result_df = inner_join_files(file_R, file_S, output_file='test_join_python.txt')
print(result_df)
