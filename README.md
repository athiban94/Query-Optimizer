# Final Project - Query-Optimizer

> Class:  CS-562-A
> Team:  Synatax Terminators

## Dependencies
* Python v3.7.1
* psycopg2
* prettytable

> Authors:
> * Sri Vallabhaneni
> * P Athiban

## Directory Structure

```
.
├── notes                       # Consists all the documentation of the queries
├── PPT                         # Power Point Presentation 
├── queries                     # Consists all the queries in the form of `.json` files
├── auto_emf.py                 # Entry point file, needs to be executed first, responsible for generating logic code for MF/EMF
├── emf_helperAggr.py           # Contains Helper functions for computing various aggregations
├── README.md                   # README file
├── requirments.txt             # Contains dependencies of the program
└── .gitignore                  # Contains files and folders to force git to untrack
```

For Linux and Mac systems `python`, `pip`, `python3`, and `pip3` are different commands. As this project depends on __Python 3__, all commands where `pip` or `python` are mentioned must use `python3` or `pip3` respectively.

In order to install the dependencies, issue the following command:

```
pip3 install -r requirements.txt
```

## Execution

In order to run the program, issue the following command:

```
python3 auto_emf.py
```

After running  the above command `bot.py` file will be generated in the same directroy which is resposible for running the query.
To see the output of the query issue the following command:

```
python3 bot.py
```

# Contributions:

## Sri Vallabhaneni

* Created the logic for sum aggregate function 
* Created the logic for average aggregate function 
* Created the logic for count aggregate function 

## P Athiban

* Created the logic for minimum aggregate function 
* Created the logic for maximum aggregate function 

## Pair Programming

* Creating the entire MF/EMF structure
* Integrating all the aggregation functions with the corresponding grouping variable and there by generating the automated code
* Outputing the query results in the form of pretty table
* Generating various queries in English, JSON, MF and EMF form
* Documenting and bug fixing
* Creating Presention (PPT)
