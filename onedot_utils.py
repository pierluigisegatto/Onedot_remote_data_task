# -*- coding: utf-8 -*-
# -----------------------------------------------------------
# this script contains a serires of functions useful for the implementation of
# the Onedot data analyst test.
# There are functions for reading, manipulating, and analysisng, and saving
# the given supplier dataset.
#
# (C) 2021 Segatto Pier Luigi, Lausanne, Switzerland
# Released under GNU Public License (GPL)
# email pier.segatto@gmail.com
# -----------------------------------------------------------

# import libraries
import pandas as pd  # library for data structures and data analysis tools
import numpy as np  # library for supporting arrays and high level math funcs.


# set display output options
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 500)


def import_target(path):
    """
    Wrapper of the common pd.read_excel() method.

    Parameters
    ----------
    path : str
        Path with the location of the customer excel target dataset.

    Returns
    -------
    pandas.DataFrame
        It contains the customer dataset in a pandas dataframe

    """
    return pd.read_excel(path)


def preprocessing_supplier_data(path_to_data):
    """
    Function that reads the supplier JSON dataset, reshapes the imported
    dataset to the same granularity of the customer dataset (over the first
    axis), and returns the resulting dataframe.

    Parameters
    ----------
    path_to_data : str
        Path to the location of the input JSON file.

    Returns
    -------
    step1_df : pandas.DataFrame
        Preprocessed dataset with the same granularity over the rows.

    """
    # Import the supplier car dataset.
    # The .json provided has to be read as a json object by line.
    # Set also the encoding to be utf-8 for decoding py3 bytes. This is
    # the default option after version 0.19.0.
    car_df = pd.read_json(path_to_data,
                          lines=True, encoding='utf-8')

    # Each row of the dataset contains a different attribute of a specific car.
    # Each row is characterized by a different entity_id whereas each car has
    # its own ID, i.e. the same ID shows up over multiple rows.

    # pivot the table to get a better representation of the dataset
    step1_df = car_df.pivot(index=["ID", "MakeText", 'TypeName',
                                   'TypeNameFull', 'ModelText',
                                   'ModelTypeText'],
                            columns=['Attribute Names'],
                            values=['Attribute Values'])

    # Get a closer representation to the customer output
    step1_df = step1_df.droplevel(0, axis=1)
    step1_df.reset_index(inplace=True)

    # the step1_df has now the same granularity as the target dataset,
    # except for the number of columns.
    print("\n")
    print("Step 1 - Preprocessing: Get the same granularity as the customer dataset")
    print(f"         Number of records to be added: {step1_df.shape[0]}")
    print(f"         Number of attributes available: {step1_df.shape[1]}")

    return step1_df


def normalize_supplier_data(step1_df, customer_df, customer_col_names):
    """
    Function that extracts from the supplier dataset the information required
    by the customer. The input customer df is analyzed column by column.
    The supplier dataframe is thus modified accordingly in order to reach the
    same quality of information.

    Parameters
    ----------
    step1_df : pandas.DataFrame
        the preprocessed supplier dataframe.
    customer_df : pandas.DataFrame
        the target supplier dataframe. It is useful for debugging and checking
        types and values.
    customer_col_names : list
        list containing the customer column labels. It is useful for debugging
        and checking types and values.

    Returns
    -------
    step1_df : pandas.DataFrame
        normalized supplier dataframe.
    """
    # To get closer to the customer dataset representation most of the attributes
    # in the supplier dataset have to be manipulated to match the format and the
    # information conveyed.
    print("\n")
    print("Step 2 - Normalization: Get the same data representation of the target.")

    # I start with the column labeled type as the carType column contains useful
    # information which will be lost in the next steps.

    # 15) type: Given that the customer is most probably selling only cars (the
    # target dataset contains only cars) I tag with 'Other' what is not a car
    # in the supplier dataset (there is a bike and a truck).
    # The bike is an HARLEY-DAVIDSON whereas the truck has BodyTypeText = Sattelschlepper.
    step1_df['type'] = 'car'
    step1_df.loc[step1_df['MakeText'] == 'HARLEY-DAVIDSON', 'type'] = 'Other'
    step1_df.loc[step1_df['BodyTypeText'] ==
                 'Sattelschlepper', 'type'] = 'Other'

    # 1) carType: corresponds to BodyTypeText in the input dataset.
    # As a firt step, I decide to associate to the supplier BodyTypes the closest
    # carType that is present in the customer dataset. If a BodyType does not fit
    # to any carType category it goes to Other. The only NaN value in the supplier
    # dataset pertains to an Harley-Davidson (a bike), hece it is set to Other.

    # NOTE: I would discuss with the customer the possibilitiy of enlarging the
    # carType dictionary to provide the final user more info about the given car.

    # print(
    #     f"\n         Car types present in the customer dataset:\n { customer_df[customer_col_names[0]].unique()}")
    # print(
    #     f"\n         Car types present in the supplier dataset:\n { step1_df['BodyTypeText'].unique()}")

    bodyTypeText_to_carType = {'Limousine': 'Custom',
                               'Kombi': 'Station Wagon',
                               'Coupé': 'Coupé',
                               'SUV / Geländewagen': 'SUV',
                               'Cabriolet': 'Convertible / Roadster',
                               'Pick-up': 'SUV'}

    # Here I use the map method to associate keys and values of my dict to the
    # supplier dataset. It is convenient because what is not in the dict is set
    # to NaN and can be easily replaced with 'Other'.
    step1_df['BodyTypeText'] = step1_df['BodyTypeText'].map(
        bodyTypeText_to_carType)
    step1_df['BodyTypeText'].fillna(value='Other', inplace=True)

    # Rename column
    step1_df.rename(columns={'BodyTypeText': 'carType'}, inplace=True)

    # 2) color: corresponds to BodyColorText in the input dataset.
    # Similarly to carType I create a dictionary to map colors of the
    # supplier database to those already present in the customer database.

    # NOTE: The dictionary can be easily enlarged if the customer desires more details to
    # be displayed.

    # print(
    #     f"\n         Colors present in the customer dataset:\n { customer_df[customer_col_names[1]].unique()}")
    # print(
    #     f"\n         Colors present in the supplier dataset:\n { step1_df['BodyColorText'].unique()}")

    bodyColorText_to_color = {'beige': 'Beige', 'beige mét.': 'Beige',
                              'blau': 'Blue', 'blau mét.': 'Blue',
                              'braun': 'Brown', 'braun mét.': 'Brown',
                              'gelb': 'Yellow', 'gelb mét.': 'Yellow',
                              'gold': 'Gold', 'gold mét.': 'Gold',
                              'grau': 'Gray', 'grau mét.': 'Gray',
                              'grün': 'Green', 'grün mét.': 'Green',
                              'orange': 'Orange', 'orange mét.': 'Orange',
                              'rot': 'Red', 'rot mét.': 'Red',
                              'schwarz': 'Black', 'schwarz mét.': 'Black',
                              'silber': 'Silver', 'silber mét.': 'Silver',
                              'violett mét.': 'Purple',
                              'weiss': 'White', 'weiss mét.': 'White'}

    step1_df['BodyColorText'] = step1_df['BodyColorText'].map(
        bodyColorText_to_color)
    step1_df['BodyColorText'].fillna(value='Other', inplace=True)

    # Rename column
    step1_df.rename(columns={'BodyColorText': 'color'}, inplace=True)

    # 3) condition: corresponds to ConditionTypeText in the input dataset.
    # Here, in absence of more detailed information I associate to the tag
    # 'Occasion', 'Oldtimer', and 'Vorführmodell' the status 'Used'.

    # NOTE: I would propose the customer to ask the supplier more detailed
    # info about the cars condition.

    # print(
    #     f"\n         Conditions present in the customer dataset:\n { customer_df[customer_col_names[2]].unique()}")
    # print(
    #     f"\n         Conditions present in the supplier dataset:\n { step1_df['ConditionTypeText'].unique()}")

    conditionTypeText_to_condition = {'Occasion': 'Used',
                                      'Oldtimer': 'Used',
                                      'Neu': 'New',
                                      'Vorführmodell': 'Used'}

    step1_df['ConditionTypeText'] = step1_df['ConditionTypeText'].map(
        conditionTypeText_to_condition)
    # step1_df['ConditionTypeText'].fillna(value='Other', inplace=True)

    # Rename column
    step1_df.rename(columns={'ConditionTypeText': 'condition'}, inplace=True)

    # 4) currency: All cars are coming from Switzerland, hence I assume the
    # currency is CHF.

    # NOTE: If possible I would ask for the selling price (it comes in handy
    # also for the price on request column).
    step1_df['currency'] = 'CHF'

    # 5) drive: Given that Swiss cars in the customer dataset can be found both
    # RHD and LHD, I won't assume anything here and I create a column filled
    # with NaNs.

    # NOTE: to dicuss with the customer about requesting more data to the supplier
    step1_df['drive'] = np.nan

    # 6) city: corresponds to City in the input dataset
    # Rename column
    step1_df.rename(columns={'City': 'city'}, inplace=True)

    # 7) country: to be created and filled with 'CH'
    step1_df['country'] = 'CH'

    # 8) make: corresponds to the MakeText column in the input dataset.
    # To provide the same output format I decide to capitalize the first letter
    # of each word while keeping the rest in lowercase.
    step1_df['MakeText'] = step1_df['MakeText'].str.title()

    # print(
    #     f"\n         Car Makers present in the customer dataset:\n { customer_df[customer_col_names[7]].sort_values().unique()}")
    # print(
    #     f"\n         Car Makers present in the supplier dataset:\n { step1_df['MakeText'].sort_values().unique()}")

    # I set in upper case all car make whose name is less than 4 chars
    # (with only few exceptions, see below).
    step1_df['MakeText'] = step1_df['MakeText'].apply(
        lambda x: x.upper() if len(x) < 4 else x)

    # Bmw-Alpina becomes Alpina
    step1_df.loc[step1_df['MakeText'] == 'Bmw-Alpina', 'MakeText'] = 'Alpina'

    # Mclaren becomes McLaren
    step1_df.loc[step1_df['MakeText'] == 'Mclaren', 'MakeText'] = 'McLaren'

    # Mini becomes MINI
    step1_df.loc[step1_df['MakeText'] == 'Mini', 'MakeText'] = 'MINI'

    # RUF becomes Ruf
    step1_df.loc[step1_df['MakeText'] == 'RUF', 'MakeText'] = 'Ruf'

    # Rename column
    step1_df.rename(columns={'MakeText': 'make'}, inplace=True)

    # 9) manufacture_year: corresponds to FirstRegYear in the input dataset
    # Change dtype from string to int64
    step1_df['FirstRegYear'] = step1_df['FirstRegYear'].astype('int64')

    # Rename column
    step1_df.rename(columns={'FirstRegYear': 'manufacture_year'}, inplace=True)

    # 10) mileage: corresponds to the Km column in the input dataset
    # Change dtype from string to float64
    step1_df['Km'] = step1_df['Km'].astype('float64')

    # Rename column
    step1_df.rename(columns={'Km': 'mileage'}, inplace=True)

    # 11) mileage_unit. I add this column filled with kilometer as this is the
    # unit provided by the supplier.
    step1_df['mileage_unit'] = 'kilometer'

    # 12) model: corresponds to the ModelText column in the input dataset
    # Rename column
    step1_df.rename(columns={'ModelText': 'model'}, inplace=True)

    # 13) model_variant: corresponds to the ModelTypeText
    step1_df.rename(columns={'ModelTypeText': 'model_variant'}, inplace=True)

    # 14) price_on_request: given that price info have not been provided I set
    # this column to be filled with True (bool) values.
    step1_df['price_on_request'] = True

    # NOTE: I would ask the customer to request price info to the supplier.
    # This would solve also the issues regarding the currency used.

    # 16) zip: I associate to each of the 6 swiss cities its canton.
    # NOTE: I would ask if this info is really necessary given the high amount
    # of missing info in the customer dataset.
    step1_df['zip'] = 'St. Gallen'
    step1_df.loc[step1_df['city'] == 'Basel', 'zip'] = 'Basel-Stadt'
    step1_df.loc[step1_df['city'] == 'Porrentrury', 'zip'] = 'Jura'
    step1_df.loc[step1_df['city'] == 'Safenwil', 'zip'] = 'Aargau'
    step1_df.loc[step1_df['city'] == 'Sursee', 'zip'] = 'Lucerne'

    # 17) manufacture_month: corresponds to the FirstRegMonth column of the
    # input dataset.
    # Change dtype
    step1_df['FirstRegMonth'] = step1_df['FirstRegMonth'].astype('float64')

    # Rename column
    step1_df.rename(
        columns={'FirstRegMonth': 'manufacture_month'}, inplace=True)

    # 18) fuel_consumption_unit. Considering that the supplier column ConsumptionRatingText
    # contains info about the required unit, and that all of them are either
    # 'null' or expressed as l/100km, I map all 'null' with NaNs (as in the
    # target dataset) and all the others with 'l_km_consumption'
    step1_df['fuel_consumption_unit'] = 'l_km_consumption'
    step1_df.loc[step1_df['ConsumptionRatingText']
                 == 'null', 'fuel_consumption_unit'] = np.nan

    print('         Supplier data adapted and ready to be integrated with the customer database.')
    return step1_df


def integrate_supplier_data(step2_df, customer_df, customer_col_names):
    """
    Function that accepts as input the normalized supplier dataframe, filters
    only the columns that are required by the customer, and vertically stack
    the customer (on top) and supplier databases.

    Parameters
    ----------
    step2_df : pandas.DataFrame
        the normalized supplier dataframe.
    customer_df : pandas.DataFrame
        the target supplier dataframe that has to be merged.
    customer_col_names : list
        list containing the customer column labels. Useful for filtering the
        columns of the supplier dataframe

    Returns
    -------
    step3_df : pandas.DataFrame
        merged customer + supplier dataframes.

    """
    print("\n")
    print("Step 3 -Integration: Merge customer and supplier databases.")

    # Filter only the interesting columns
    filtered_df = step2_df[customer_col_names]

    # Vertically stack the customer and the supplier dfs.
    step3_df = pd.concat([customer_df, filtered_df], ignore_index=True)

    # NOTE: I would ask to the customer how he prefer to treat NaNs and if he
    # wants me to beautify their own database.
    print("         Datasets concatenated.")

    return step3_df


def write_excel_output(df1, df2, df3, path='output/output.xlsx'):
    """
    Funtion that writes to an excel workbook the partial and final results of
    the Onedot remote task. Each one of the input dataframes are saved in the
    same file but on a different worksheet. The visual result is slightly
    improved enlarging thin columns and adding an autofilter to each table.

    Parameters
    ----------
    df1 : pandas.DataFrame
        preprocessed supplier dataframe.
    df2 : pandas.DataFrame
        normalized supplier dataframe.
    df3 : pandas.DataFrame
        integrated supplier + customer dataframe.
    path : str, optional
        path to the location of where the output has to be saved.
        The default is 'output/output.xlsx'.

    Returns
    -------
    None.

    """
    # create excel writer
    writer = pd.ExcelWriter(path, engine='xlsxwriter')

    # write dataframe to excel sheet named 'Step 1 - Preprocessing'
    df1.to_excel(writer, sheet_name='Step 1 - Preprocessing',
                 index=False, header=True)

    # Get access to the workbook and to the worksheet
    workbook = writer.book
    worksheet = writer.sheets['Step 1 - Preprocessing']

    # Add a header format.
    header_format = workbook.add_format({
        'bold': False})

    # Write the column headers with the defined format.
    for col_num, col_name in enumerate(df1.columns.values):
        worksheet.write(0, col_num, col_name, header_format)

    # Set zoom for better visibility on big monitors
    worksheet.set_zoom(100)

    # Car name columns
    worksheet.set_column('B:F', 30)

    # Car color and bodytype columns
    worksheet.set_column('G:H', 15)

    # Car properties column
    worksheet.set_column('W:W', 30)

    # Transmission type column
    worksheet.set_column('Y:Y', 30)

    # Add the autofilter capability to the 2D database
    worksheet.autofilter('A1:Y1154')

    # write dataframe to excel sheet named 'Step 2 - Normalization'
    df2.to_excel(writer, sheet_name='Step 2 - Normalization',
                 index=False, header=True)

    # Get access to the workbook and to the worksheet
    worksheet = writer.sheets['Step 2 - Normalization']

    # Write the column headers with the defined format.
    for col_num, col_name in enumerate(df2.columns.values):
        worksheet.write(0, col_num, col_name, header_format)

    # Set zoom for better visibility on big monitors
    worksheet.set_zoom(100)

    # Car name columns
    worksheet.set_column('B:F', 30)

    # Car color and bodytype columns
    worksheet.set_column('G:H', 15)

    # Car properties column
    worksheet.set_column('W:W', 30)

    # Transmission type column
    worksheet.set_column('Y:Y', 30)

    # mileage column
    worksheet.set_column('AD:AD', 15)

    # price on request column
    worksheet.set_column('AE:AE', 15)

    # fuel consumption unit column
    worksheet.set_column('AG:AG', 20)

    # Add the autofilter capability to the 2D database
    worksheet.autofilter('A1:AG1154')

    # write dataframe to excel sheet named 'Step 3 - Integration'
    df3.to_excel(writer, sheet_name='Step 3 - Integration',
                 index=False, header=True)

    # Get access to the workbook and to the worksheet
    worksheet = writer.sheets['Step 3 - Integration']

    # Write the column headers with the defined format.
    for col_num, col_name in enumerate(df3.columns.values):
        worksheet.write(0, col_num, col_name, header_format)

    # Set zoom for better visibility on big monitors
    worksheet.set_zoom(100)

    # Car model columns
    worksheet.set_column('L:M', 30)

    # Car type column
    worksheet.set_column('A:A', 25)

    # Car color and condition columns
    worksheet.set_column('B:C', 15)

    # City column
    worksheet.set_column('F:F', 20)

    # fuel consumption column
    worksheet.set_column('R:R', 25)

    # Add the autofilter capability to the 2D database
    worksheet.autofilter('A1:R8406')

    # save the excel file
    writer.save()

    print(f"\nWorksheet created and saved in {path}")
