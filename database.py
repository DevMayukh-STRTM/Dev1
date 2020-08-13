import os
import datetime
import pickle
import csv

#(C)2020 Syantace Softwares, Inc. All Rigths Reserved
#V3.0 What's New?
#     Reduced redundancy, added class methods to operate multiple databases at the same time
#     Easy to operate, manage and high readabilty
#     New Data Format And Quick Save Feature
#     Added Console for direct operation
#     Manual Creation of database - Create file with .stdb extention

#     Structure:
# {
#     "meta": {
#         "name": "database_name",
#         "date": "date created",
#         "password": None
#     },

#     "table_name": {
#         "meta": {
#             "name": "table_name",
#             "date": "date created",
#             "total_columns": 0
#         },
#         "data": {
#             "column_name": {
#                 "meta": {
#                     "name": "column_name",
#                     "date": "date created",
#                     "data_type": "STRING"
#                 },
#                 "data": {
#                     1: "first_input"
#                 }
#             }
#         }
#     }
# }

#universal set of types:

pos_types = ["INTEGER", "FLOAT", "STRING", "BOOL", "LIST", "TUPLE", "DICTIONARY"]

class database:
    def __init__(self, forDat):
        self.credintial = forDat
    #first function create database:

    def createDatabase(self):
        filename = self.credintial[0]
        password = self.credintial[1]
        if os.path.isfile(f"{filename}.stdb"):
            return "Database Already Exist"
        else:
            general_structure = {
                "meta": {
                    "name": f"{filename}",
                    "date": f"{datetime.datetime.now()}",
                    "password": f"{password}"
                }
            }
            pickle.dump(general_structure, open(f"{filename}.stdb", 'wb'))
            return "Successfully Created Database"

    #second function create table:

    def createTable(self, table_name):
        filename = self.credintial[0]
        password = self.credintial[1]
        if os.path.isfile(f"{filename}.stdb"):
            ax = pickle.load(open(f'{filename}.stdb', 'rb'))
            if ax["meta"]["password"] == str(password):
                if table_name in ax:
                    return "Table Already Exists"
                else:
                    general_Structure = {
                        "meta": {
                            "name": f"{table_name}",
                            "date": f"{datetime.datetime.now()}",
                            "total_columns": 0
                        },
                        "data": {

                        }
                    }

                    ax[f"{table_name}"] = general_Structure
                    pickle.dump(ax, open(f"{filename}.stdb", 'wb'))
                    return "Successfully Created Table"

            elif ax["meta"]["password"] == None:
                if table_name in ax:
                    return "Table Already Exists"
                else:
                    general_Structure = {
                        "meta": {
                            "name": f"{table_name}",
                            "date": f"{datetime.datetime.now()}",
                            "total_columns": 0
                        },
                        "data": {

                        }
                    }

                    ax[f"{table_name}"] = general_Structure
                    pickle.dump(ax, open(f"{filename}.stdb", 'wb'))
                    return "Successfully Created Table"
            else:
                return "Wrong Password"
        else:
            return "Invalid Database"


    #simultaneous table creation

    def createTableFromList(self, tables):
        filename = self.credintial[0]
        password = self.credintial[1]

        if os.path.isfile(f"{filename}.stdb"):
            ax = pickle.load(open(f"{filename}.stdb", 'rb'))
            if ax["meta"]["password"] == password:
                for x in list(tables):
                    if x in ax:
                        return "Table Already Exists"
                    else:
                        general_Structure = {
                            "meta": {
                                "name": f"{x}",
                                "date": f"{datetime.datetime.now()}",
                                "total_columns": 0
                            },
                            "data": {

                            }
                        }

                        ax[f"{x}"] = general_Structure
                        
                pickle.dump(ax, open(f"{filename}.stdb", 'wb'))
                return "Successfully Created Table"
                    

            elif ax["meta"]["password"] == "None":
                for x in tables:
                    if x in ax:
                        return "Table Already Exists"
                    else:
                        general_Structure = {
                            "meta": {
                                "name": f"{x}",
                                "date": f"{datetime.datetime.now()}",
                                "total_columns": 0
                            },
                            "data": {

                            }
                        }

                        ax[f"{x}"] = general_Structure

                pickle.dump(ax, open(f"{filename}.stdb", 'wb'))
                return "Successfully Created Table"
            
            else:
                return "Invalid Password"

        else:
            return f"Database non-existing, {filename}"
    def createCol(self, attr):
        database = self.credintial[0]
        password = self.credintial[1]

        table = attr[0]
        col_Name = attr[1]
        type_c = attr[2]

        if os.path.isfile(f"{database}.stdb"):
            ax = pickle.load(open(f"{database}.stdb", 'rb'))
            general_structure = {
                "meta": {
                     "name": f"{col_Name}",
                     "date": f"{datetime.datetime.now()}",
                     "data_type": f"{type_c}"
                 },
                 "data": {
                }
            }
            if table in ax:
                if col_Name in ax[f"{table}"]["data"]:
                    return "Column Already Exist"
                else:
                    if type_c == pos_types[0]:
                        ax[f"{table}"]["data"][f"{col_Name}"] = general_structure
                    elif type_c == pos_types[1]:
                        ax[f"{table}"]["data"][f"{col_Name}"] = general_structure
                    elif type_c == pos_types[2]:
                        ax[f"{table}"]["data"][f"{col_Name}"] = general_structure
                    elif type_c == pos_types[3]:
                        ax[f"{table}"]["data"][f"{col_Name}"] = general_structure
                    elif type_c == pos_types[4]:
                        ax[f"{table}"]["data"][f"{col_Name}"] = general_structure
                    elif type_c == pos_types[5]:
                        ax[f"{table}"]["data"][f"{col_Name}"] = general_structure
                    elif type_c == pos_types[6]:
                        ax[f"{table}"]["data"][f"{col_Name}"] = general_structure
                    else:
                        return "Invalid type"
                        exit()
                    
                    ax[f"{table}"]["meta"]["total_columns"] += 1
                    pickle.dump(ax, open(f"{database}.stdb", 'wb'))
                    return "Created New Column"
            else:
                return "Table is non-existing"
        else:
            return "Invalid Database"
    
    def createColFromList(self, attr):
        database = self.credintial[0]
        password = self.credintial[1]

        #attr = ["table_name", [(colname, type), (colname, type)]]

        col_name = attr[1]
        table = attr[0]

        if os.path.isfile(f"{database}.stdb"):
            ax = pickle.load(open(f"{database}.stdb", 'rb'))
            if attr[0] in ax:
                for col_Name in col_name:
                    type_c = col_Name[1]
                    if type_c == pos_types[0]:
                        ax[f"{table}"]["data"][f"{col_Name[0]}"] = {"meta": {"name": f"{col_Name[0]}", "date": f"{datetime.datetime.now()}", "data_type": f"{type_c}"}, "data": {}}
                    elif type_c == pos_types[1]:
                        ax[f"{table}"]["data"][f"{col_Name[0]}"] = {"meta": {"name": f"{col_Name[0]}", "date": f"{datetime.datetime.now()}", "data_type": f"{type_c}"}, "data": {}}
                    elif type_c == pos_types[2]:
                        ax[f"{table}"]["data"][f"{col_Name[0]}"] = {"meta": {"name": f"{col_Name[0]}", "date": f"{datetime.datetime.now()}", "data_type": f"{type_c}"}, "data": {}}
                    elif type_c == pos_types[3]:
                        ax[f"{table}"]["data"][f"{col_Name[0]}"] = {"meta": {"name": f"{col_Name[0]}", "date": f"{datetime.datetime.now()}", "data_type": f"{type_c}"}, "data": {}}
                    elif type_c == pos_types[4]:
                        ax[f"{table}"]["data"][f"{col_Name[0]}"] = {"meta": {"name": f"{col_Name[0]}", "date": f"{datetime.datetime.now()}", "data_type": f"{type_c}"}, "data": {}}
                    elif type_c == pos_types[5]:
                        ax[f"{table}"]["data"][f"{col_Name[0]}"] = {"meta": {"name": f"{col_Name[0]}", "date": f"{datetime.datetime.now()}", "data_type": f"{type_c}"}, "data": {}}
                    elif type_c == pos_types[6]:
                        ax[f"{table}"]["data"][f"{col_Name[0]}"] = {"meta": {"name": f"{col_Name[0]}", "date": f"{datetime.datetime.now()}", "data_type": f"{type_c}"}, "data": {}}
                    else:
                        return "Invalid type"
                        exit()

                    ax[f"{table}"]["meta"]["total_columns"] += 1
                pickle.dump(ax, open(f"{database}.stdb", 'wb'))
                return "Success"
            else:
                return "Invalid Table"
        else:
            return "Invalid Database"
    
    def dynamic(self, attr):
        database = self.credintial[0]
        password = self.credintial[1]

        # here, you can dynamically create table, allocate a column and so forth
        # [("table", [("col_name", "type")])]

        #steps?

        #first, create tables in a loop, then inside a loop of the main loop, allocate the columns
        table_name = []
        col_names = []
        for tables in attr:
            table_name.append(tables[0])
            col_names.append(tables[1])
        
        self.createTableFromList(table_name)
        
        for (x, y) in zip(table_name, col_names):
            codec = [f'{x}', y]
            self.createColFromList(codec)

    def addData(self, attr):
        database = self.credintial[0]
        password = self.credintial[1]

        #attribute for adding data: ["table_name", "col_name", "data"]

        table = attr[0]
        column = attr[1]
        data = attr[2]

        if os.path.isfile(f"{database}.stdb"):
            ax = pickle.load(open(f"{database}.stdb", 'rb'))
            if table in ax:
                if pickle.load(open(f"{database}.stdb", 'rb'))["meta"]["password"] == password:
                    if column in ax[f'{table}']["data"]:
                        key = list(ax[f"{table}"]["data"][f"{column}"]["data"].keys())
                        new_key = None
                        if len(key) == 0:
                            new_key = 1
                        elif len(key) > 0:
                            new_key = len(key) + 1
                        type_c = ax[f"{table}"]["data"][f'{column}']["meta"]["data_type"]
                        try:
                            if type_c == pos_types[0]:
                                ax[f"{table}"]["data"][f'{column}']["data"][new_key] = int(data)
                            elif type_c == pos_types[1]:
                                ax[f"{table}"]["data"][f'{column}']["data"][new_key] = float(data)
                            elif type_c == pos_types[2]:
                                ax[f"{table}"]["data"][f'{column}']["data"][new_key] = str(data)
                            elif type_c == pos_types[3]:
                                ax[f"{table}"]["data"][f'{column}']["data"][new_key] = bool(data)
                            elif type_c == pos_types[4]:
                                ax[f"{table}"]["data"][f'{column}']["data"][new_key] = list(data)
                            elif type_c == pos_types[5]:
                                ax[f"{table}"]["data"][f'{column}']["data"][new_key] = tuple(data)
                            elif type_c == pos_types[6]:
                                ax[f"{table}"]["data"][f'{column}']["data"][new_key] = dict(data)
                            else:
                                return "Invalid Data type"
                        except ValueError:
                            return "Invalid data type"
                        
                        pickle.dump(ax, open(f"{database}.stdb", 'wb'))
                        return True
                    else:
                        return "Invalid column"
                elif pickle.load(open(f"{database}.stdb", 'rb'))["meta"]["password"] == 'None':
                    if column in ax[f'{table}']["data"]:
                        key = list(ax[f"{table}"]["data"][f"{column}"]["data"].keys())
                        new_key = None
                        if len(key) == 0:
                            new_key = 1
                        elif len(key) > 0:
                            new_key = len(key) + 1
                        type_c = ax[f"{table}"]["data"][f'{column}']["meta"]["data_type"]
                        try:
                            if type_c == pos_types[0]:
                                ax[f"{table}"]["data"][f'{column}']["data"][new_key] = int(data)
                            elif type_c == pos_types[1]:
                                ax[f"{table}"]["data"][f'{column}']["data"][new_key] = float(data)
                            elif type_c == pos_types[2]:
                                ax[f"{table}"]["data"][f'{column}']["data"][new_key] = str(data)
                            elif type_c == pos_types[3]:
                                ax[f"{table}"]["data"][f'{column}']["data"][new_key] = bool(data)
                            elif type_c == pos_types[4]:
                                ax[f"{table}"]["data"][f'{column}']["data"][new_key] = list(data)
                            elif type_c == pos_types[5]:
                                ax[f"{table}"]["data"][f'{column}']["data"][new_key] = tuple(data)
                            elif type_c == pos_types[6]:
                                ax[f"{table}"]["data"][f'{column}']["data"][new_key] = dict(data)
                            else:
                                return "Invalid Data type"
                        except ValueError:
                            return "Invalid data type"
                        
                        pickle.dump(ax, open(f"{database}.stdb", 'wb'))
                        return True
                    else:
                        return "Invalid column"
                else:
                    return "Invalid Password"
            else:
                return "Invalid Table"
        else:
            return "Invalid Databse"

    def addDataFromList(self, attr):
        # the new attr: [["table", "col_name", "data"], ["table", "col", "data"]]

        for x in attr:
            self.addData(x)

    def getDataByID(self, attr):
        database = self.credintial[0]
        password = self.credintial[1]

        #attribute: ["table_name", "column_name", ID]

        table = attr[0]
        column = attr[1]
        id_ = attr[2]

        if os.path.isfile(f"{database}.stdb"):
            ax = pickle.load(open(f"{database}.stdb", 'rb'))
            if table in ax:
                if pickle.load(open(f"{database}.stdb", 'rb'))["meta"]["password"] == password:
                    if column in ax[f"{table}"]["data"]:
                        if id_ in ax[f"{table}"]["data"][f'{column}']['data']:
                            return ax[f"{table}"]["data"][f'{column}']['data'][id_]
                        else:
                            return "Invalid ID"
                    else: 
                        return "Invalid Row"
                elif pickle.load(open(f"{database}.stdb", 'rb'))["meta"]["password"] == 'None':
                    if column in ax[f"{table}"]["data"]:
                        if id_ in ax[f"{table}"]["data"][f'{column}']['data']:
                            return ax[f"{table}"]["data"][f'{column}']['data'][id_]
                        else:
                            return "Invalid ID"
                    else: 
                        return "Invalid Row"
                else:
                    return "Invalid Password"
            else:
                return "Invalid Table"
        else:
            return "Invalid Database"

    def getTableData(self, table):
        database = self.credintial[0]
        password = self.credintial[1]

        if os.path.isfile(f"{database}.stdb"):
            col = []
            data = []
            ax = pickle.load(open(f"{database}.stdb", 'rb'))
            if ax["meta"]["password"] == password or ax["meta"]["password"] == 'None':
                if table in ax:
                    if len(ax[f"{table}"]["data"]) == 0 or len(ax[f"{table}"]["data"]) <= 0:
                        return []
                    else:
                        col = list(ax[f"{table}"]["data"].keys())
                        for x in range(len(col)):
                            data.append((f'{col[x]}', list(ax[f"{table}"]["data"][f'{col[x]}']["data"].values())))
                        return data
                else:
                    return "Invalid Table"
            else:
                return "Invalid Password"
        else:
            return "Invalid Database"
    



    def checkDb(self):

        database = self.credintial[0]
        password = self.credintial[1]

        if os.path.isfile(f"{database}.stdb"):
            return True
        else:
            return False
    
    def checkTb(self, table):

        database = self.credintial[0]

        if os.path.isfile(f"{database}.stdb"):
            if table in pickle.load(open(f"{database}.stdb", 'rb')):
                return True
            else:
                return False
        else:
            return "Invalid Database"

    def checkCol(self, attr):

        database = self.credintial[0]

        table = attr[0]
        column = attr[1]

        if os.path.isfile(f"{database}.stdb"):
            if table in pickle.load(open(f"{database}.stdb", 'rb')):
                if column in pickle.load(open(f"{database}.stdb", 'rb'))[f"{table}"]["data"]:
                    return True

                else:
                    return False
            else:
                return False
        else:
            return False

                    
                    







