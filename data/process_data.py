import sys
import pandas as pd
from sqlalchemy import create_engine


def load_data(messages_filepath, categories_filepath):
    '''
    read & transform & combine data
    INPUT: messages & categories filepath
    OUTPUT: transformed & combined dataframe
    '''
    # read messages and categories files & set id as index
    messages = pd.read_csv(messages_filepath).set_index('id')
    categories = pd.read_csv(categories_filepath).set_index('id')

    # create a dataframe of the 36 individual category columns
    categories = categories.categories.str.split(';', expand=True)

    # create & set categorie names
    row = categories.iloc[0, :]
    category_colnames = row.apply(lambda x: x[:-2]).tolist()
    categories.columns = category_colnames

    # convert category values to just numbers 0 or 1
    categories = categories.applymap(lambda x: x[-1]).astype(int)

    # combine messages & new categories into df
    df = messages.merge(categories, how='left', on='id')

    return df


def clean_data(df):
    '''
    clean up the dataframe
    '''
    # drop duplicates
    df = df.drop_duplicates(keep='first')

    return df


def save_data(df, database_filename):
    '''
    save processed data into database
    '''
    engine = create_engine('sqlite:///InsertDatabaseName.db')
    df.to_sql('DisasterResponse', engine, index=False)


def main():
    if len(sys.argv) == 4:

        messages_filepath, categories_filepath, database_filepath = \
            sys.argv[1:]

        print('Loading data...\n    MESSAGES: {}\n    CATEGORIES: {}'
              .format(messages_filepath, categories_filepath))
        df = load_data(messages_filepath, categories_filepath)

        print('Cleaning data...')
        df = clean_data(df)

        print('Saving data...\n    DATABASE: {}'.format(database_filepath))
        save_data(df, database_filepath)

        print('Cleaned data saved to database!')

    else:
        print('Please provide the filepaths of the messages and categories '\
              'datasets as the first and second argument respectively, as '\
              'well as the filepath of the database to save the cleaned data '\
              'to as the third argument. \n\nExample: python process_data.py '\
              'disaster_messages.csv disaster_categories.csv '\
              'DisasterResponse.db')


if __name__ == '__main__':
    main()
