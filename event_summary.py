import pandas as pd
import csv
import logging

def get_all_aliases(user_input_path, alias_input_path):
    """
    Takes the user and alias data paths as inputs and creates 
    a dictionary mapping all aliases (keys) to their original user_id (values)
    """
    logging.info('fcn:get_all_aliases: Reading user data from: {}'.format(user_input_path))
    try: 
        users = pd.read_csv(user_input_path)
        # Extracting all unique user_ids, confirms no duplicates for lookup
        uids = set(users['user_id'])
    except IOError as e:
        logging.error(e)

    alias_lookup = {}
    no_initial_match = []
    try:
        logging.info('fcn:get_all_aliases: Reading alias data from: {}'.format(alias_input_path))
        with open(alias_input_path, 'r') as alias_file:
            aliases = csv.reader(alias_file)
            
            # check the first row to ensure that we are referencing the correct columns
            headers = next(aliases) 
            if headers != ['timestamp', 'user_id', 'alias_user_id']:
                raise Exception("""Alias dataset headers don't match expected. 
                    Expected: ['timestamp', 'user_id', 'alias_user_id']
                    Actual: {}
                    """.format(headers))
            else:
                """
                Checks the user_id in the alias row to see if it is an actual uid or an alias
                and records it in the alias_lookup as such
                """
                for row in aliases:
                    if row[1] in uids:
                        alias_lookup[row[2]] = row[1]
                    elif row[1] in alias_lookup:
                        alias_lookup[row[2]] = alias_lookup[row[1]]
                    else:
                        """
                        In the case that an alias is in the aliases dataset before
                        the user_id, the alias will not make it into the lookup dictionary.
                        This saves said rows that don't have a uid match yet to be processed 
                        once all uids from the alias dataset have been 
                        """
                        no_initial_match.append(row)

        # process no_initial_matches
        if len(no_initial_match) > 0:
            for row in no_initial_match:
                if row[1] in uids:
                    alias_lookup[row[2]] = row[1]
                elif row in alias_lookup:
                    alias_lookup[row[2]] = alias_lookup[row[1]]
    except IOError as e:
        logging.error(e)
    
    logging.info('fcn:get_all_aliases: Alias lookup successfully created')
    return alias_lookup

def get_events_summary(events_input_path, alias_lookup, summary_output_path):
    '''
    Calculates an event summary with the input events that shows the user count 
    grouped by feature_key and feature_value. 
    '''
    logging.info('fcn:get_events_summary: Reading events data from: {}'.format(events_input_path))
    try: 
        events = pd.read_csv(events_input_path)
        # mapping user_id in events to actual uid
        events['uid'] = events['user_id'].map(alias_lookup)
        # uid column will be null if actual uid is recorded in events, so filling from user_id column in events
        events.uid.fillna(events.user_id, inplace = True)
        
        logging.info('fcn:get_events_summary: Calculating summary of events')
        try:
            events[['uid', 'feature_key', 'feature_value']].groupby(['feature_key', 'feature_value']).count().to_csv(summary_output_path)
            logging.info('fcn:get_events_summary: Wrote event summary to: {}'.format(summary_output_path))
        except IOError as e:
            logging.error(e)
    except IOError as e:
        logging.error(e)



def main(user_input_path, alias_input_path, events_input_path, summary_output_path):
    alias_lookup_dict = get_all_aliases(user_input_path, alias_input_path)
    get_events_summary(events_input_path, alias_lookup_dict, summary_output_path)

if __name__ == "__main__":
    logging.basicConfig(level = logging.INFO)

    USER_INPUT_PATH = 'input_data/users.csv'
    ALIAS_INPUT_PATH = 'input_data/alias.csv'
    EVENTS_INPUT_PATH = 'input_data/events.csv'
    SUMMARY_OUTPUT_PATH = 'output/event_summary.csv'

    logging.info("fcn:main: Running with the following inout/output paths:")
    logging.info("USER_INPUT_PATH: {}".format(USER_INPUT_PATH))
    logging.info("ALIAS_INPUT_PATH: {}".format(ALIAS_INPUT_PATH))
    logging.info("EVENTS_INPUT_PATH: {}".format(EVENTS_INPUT_PATH))
    logging.info("SUMMARY_OUTPUT_PATH: {}".format(SUMMARY_OUTPUT_PATH))

    main(USER_INPUT_PATH, ALIAS_INPUT_PATH, EVENTS_INPUT_PATH, SUMMARY_OUTPUT_PATH)
