# Final Project - Query-Optimizer

> Class:  CS-562-A
> Team:  Syntax Terminators

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
python3 auto_emf.py -f <query_file_name.json>
```

After running  the above command `bot.py` file will be generated in the same directroy which is resposible for running the query.
To see the output of the query issue the following command:

```
python3 bot.py
```

## How to create your own queries

You should create a .json file in the `./queries` directory by creating the following parameters to the
`phi` operator as key and value

## EMF
```
select : list of projection attributes
n      : number of grouping variables
v      : list of grouping attributes
f      : list of aggregate functions
st     : list of such that conditions
g      : string containing the having condition
```


## Example of EMF
```
{
    "select": [
        "cust", "prod", "1_max_quant", "2_max_quant", "3_max_quant"
    ],
    "n": 3,

    "v": [
        "prod", "cust"
    ],
    "f": [
        "1_max_quant", "2_max_quant", "3_max_quant"
    ],

    "st" :[
       "1_prod = prod", "1_cust = cust", "1_state = NY",
       "2_prod = prod", "2_cust = cust", "2_state = NJ",
       "3_prod = prod", "3_cust = cust", "3_state = CT"
    ],

    "g" : "1_max_quant < 2_max_quant and 1_max_quant < 3_max_quant"
}

```


## MF 
```
mf     : mf query
select : list of projection attributes
n      : number of grouping variables
v      : list of grouping attributes
f      : list of aggregate functions
st     : list of such that conditions
g      : string containing the having condition
```


## Example of MF
```
{
    "mf" : "mf_query",
    "select": [
        "cust",
        "1_avg_quant",
        "2_avg_quant",
        "3_avg_quant"
    ],
    "n": 3,
    "v": [
        "cust"
    ],
    "f": [
        "1_avg_quant",
        "2_avg_quant",
        "3_avg_quant"
    ],
    "st": [
        "1_state = NY",
        "2_state = NJ",
        "3_state = CT"
    ],
    "g": "1_avg_quant > 2_avg_quant and 1_avg_quant > 3_avg_quant"
}
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

# Works Cited & Consulted

Dr. Samuel Kim, Computer Science CS562. Stevens Institute of Technology. 7 May 2020.