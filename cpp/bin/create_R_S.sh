#!/bin/bash

# Creates partioned input zipf data files

# Check if the correct number of arguments is passed
if [ "$#" -ne 5 ]; then
    echo "Usage: $0 <seed> <alpha> <n_unique> <n_rows> <n_servers>"
    exit 1
fi

# Arguments for gen_zipf
seed=$1
alpha=$2
n_unique=$3
n_rows=$4
n_servers=$5

# Create zipf data
./gen_zipf $seed $alpha $n_unique $n_rows

# Format the arguments for file names
formatted_alpha=$(echo $alpha | tr '.' 'p')

# Generate file names based on the arguments
zipf_file="zipf_${seed}_${formatted_alpha}_${n_unique}_${n_rows}.txt"
s_zipf_file="S_${zipf_file}"

# Create S table with additional float column
./add_row_numbers $zipf_file S
# # Split S into sub-files according to number of servers
./split_file $s_zipf_file $n_servers

# Create filenames for the generated R data files
gen_r_file="${n_unique}.txt"
r_file="R_${gen_r_file}"

# Generate R
./gen_R $n_unique

# Create R table with additional float column
./add_row_numbers $gen_r_file R
# Split R into sub-files according to number of servers
./split_file $r_file $n_servers