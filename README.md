# mg_de_fitbod

### Run Notes:
- Running the `event_summary.py` job will require the user, events and alias dataset csvs and will output a csv called `event_summary.csv` that is saved in an `output` directory. This directory should already exist, if it doesn't, please make the directory prior to running the script. 
- To execute the `event_summary.py` script, ensure that the user, alias and events dataset csvs are available and saved in the `input_data` directory one below where the script is executing and are named `users.csv`, `alias.csv` and `events.csv` respectively. 
- The script assumes the files are saved in the location and name as appears in this repo. If that will be different for your execution, update lines 95-98 of `event_summary.py` to point to the proper location of the input data. This is a potential enhancement to take the file locations as arguments in the script itself, however was not built into the current job. 
