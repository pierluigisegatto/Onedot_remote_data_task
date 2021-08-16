# -*- coding: utf-8 -*-
# -----------------------------------------------------------
# demonstrates how to analyse, manipulate, and summarise e-commerce suppliers data.
# To run this script from command line type: “python run.py”
#
# (C) 2021 Segatto Pier Luigi, Lausanne, Switzerland
# Released under GNU Public License (GPL)
# email pier.segatto@gmail.com
# -----------------------------------------------------------

from onedot_utils import import_target, preprocessing_supplier_data
from onedot_utils import normalize_supplier_data, integrate_supplier_data, write_excel_output


def main():
    """
    Main program function. This function is called at startup. It manages the
    workflow of this exercise for the Onedot company. It loads and processes
    the required supplier data. Data are then manipulated in order to output
    to the user the target information. An excel file named output.xlsx,
    showing the final and partial results is also created and saved in the
    output folder.

    Returns
    -------
    preprocessed_data : pandas.DataFrame
        Formatted df containing the raw supplier data with the same granularity as
        the customer database.

    normalized_data : pandas.DataFrame
        Formatted df containing all normalized attributes provided by the supplier
        along with the missing columns of the customer dataset. Data are
        normalized to match the same style and degree of information required by
        the customer.

    integrated_data : pandas.DataFrame
        Formatted df containing the customer and supplier dataset vertically
        concatenated

    """
    # =========================================================================
    # 0 - Step: Import the customer dataset and check datatypes
    # =========================================================================

    # import customer df
    customer_df = import_target('data/Target Data.xlsx')

    # store the column names for later usage
    customer_col_names = customer_df.columns.tolist()

    # Print the column names and datatypes
    print("\n")
    print("Step 0 - Preliminary check: Import and look at the customer dataset")
    print(
        f"         The provided dataset has to be adapted to reproduce the following information: \n{customer_df.dtypes}")

    # =========================================================================
    # 1 - Step Preprocessing
    # =========================================================================
    # Objective: transform the supplier data to achieve the same granularity
    # as the customer data.

    # Call the preprocessing function
    step1_df = preprocessing_supplier_data('data/supplier_car.json')

    # Make an hard copy to prevent modifications
    preprocessed_data = step1_df.copy(deep=True)
    # =========================================================================
    # 2 - Step Normalization
    # =========================================================================
    # Objective: Normalisation is required in case an attribute value is
    # different but actually is the same (different spelling, language,
    # different unit used etc.).

    # Please normalise at least 2 attributes, and describe which normalisations
    # are required for the other attributes including examples.

    # Call the normalize function
    step2_df = normalize_supplier_data(
        step1_df, customer_df, customer_col_names)

    # Make an hard copy to prevent modifications
    normalized_data = step2_df.copy(deep=True)

    # =========================================================================
    # 3 - Step Integration
    # =========================================================================
    # Objective: Data Integration is to transform the supplier data with a
    # specific data schema into a new dataset with target data schema, such as to:
    # - keep any attributes that you can map to the target schema
    # - discard attributes not mapped to the target schema
    # - keep the number of records as unchanged

    # Call the integrate function
    integrated_data = integrate_supplier_data(step2_df,
                                              customer_df, customer_col_names)

    # =========================================================================
    # 4 - Save the output to excel
    # =========================================================================

    # Call the customized save function
    write_excel_output(preprocessed_data,
                       normalized_data,
                       integrated_data,
                       path='output/output.xlsx')

    print("\nDONE")
    return preprocessed_data, normalized_data, integrated_data


if __name__ == "__main__":
    # run main program
    step1_df, step2_df, step3_df = main()
