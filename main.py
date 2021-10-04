from __future__ import absolute_import, print_function

import pandas
import argparse as ap
import numpy as np
from io import StringIO

import itertools as it

from meza.io import read_csv, IterStringIO
from csv2ofx import utils
from csv2ofx.ofx import OFX
from csv2ofx.mappings.capitalone import mapping


def insert_row(row_number, df, row_value):
    # Function to insert row in the dataframe
    # Starting value of upper half
    start_upper = 0

    # End value of upper half
    end_upper = row_number

    # Start value of lower half
    start_lower = row_number

    # End value of lower half
    end_lower = df.shape[0]

    # Create a list of upper_half index
    upper_half = [*range(start_upper, end_upper, 1)]

    # Create a list of lower_half index
    lower_half = [*range(start_lower, end_lower, 1)]

    # Increment the value of lower half by 1
    lower_half = [x.__add__(1) for x in lower_half]

    # Combine the two lists
    index_ = upper_half + lower_half

    # Update the index of the dataframe
    df.index = index_

    # Insert a row at the end
    df.loc[row_number] = row_value

    # Sort the index labels
    df = df.sort_index()

    # return the dataframe
    return df


def main():
    parser = ap.ArgumentParser(
        prog='main.py',
        description='Converter of JetLend Excel')

    parser.add_argument(
        '--input',
        help='Input Excel file',
        metavar='input',
        required=True)

    parser.add_argument(
        '--output',
        help='Output CSV file',
        metavar='output',
        required=True)

    args = parser.parse_args()
    df = pandas.read_excel(args.input, sheet_name='Sheet1')

    # print whole sheet data
    df = df.drop(df.columns[[0]], axis=1)
    df[df.columns[[3, 4, 5, 6]]] = df[df.columns[[3, 4, 5, 6]]].fillna(0)
    df[df.columns[[2]]] = df[df.columns[[2]]].fillna('Top-up')

    df.columns = ['Posted Date', 'Category',
                  'Description', 'Credit', 'Debit', 'Debt', 'Income']
    df['Transaction Date'] = df['Posted Date']
    df['Card No.'] = 12345

    cols = df.columns.tolist()
    cols = [cols[7], cols[0], cols[8], cols[2],
            cols[1], cols[3], cols[4], cols[5], cols[6]]
    df = df[cols]

    df = df.drop(df[df.Category == 'investment'].index)

    for index, row in df.iterrows():
        if row['Debt'] != 0:
            row['Credit'] = row['Debt']
            df.at[index, 'Credit'] = row['Debt']
            df.at[index, 'Debt'] = 0
            df.at[index, 'Category'] = 'contract'

    for index, row in df.iterrows():
        if row['Income'] != 0:
            row['Credit'] = row['Income']
            row['Income'] = 0
            row['Category'] = 'income'
            df = insert_row(index, df, row)

    df = df.drop(df.columns[[7, 8]], axis=1)
    df = df.replace({0: np.nan})

    print(df)

    # csv = df.to_csv(args.output, index=False, sep=",", decimal=".")
    csv = df.to_csv(index=False, sep=",", decimal=".")
    # print(csv)
    f = StringIO(csv)

    ofx = OFX(mapping)
    ofx.currency = 'RUB'
    records = read_csv(f, dedupe=True)
    groups = ofx.gen_groups(records)
    trxns = ofx.gen_trxns(groups)
    cleaned_trxns = ofx.clean_trxns(trxns)
    data = utils.gen_data(cleaned_trxns)
    content = it.chain([ofx.header(), ofx.gen_body(data), ofx.footer()])

    with open(args.output, 'wb') as file:
        for line in IterStringIO(content):
            file.write(line)
            file.write(b'\n')


if __name__ == '__main__':
    main()
