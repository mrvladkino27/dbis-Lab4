from config import Connect,NamePath_files
from bson.son import SON
from time_wraper import profile_time
import csv
import time

step = 10000
Max_Amount_To_Upload = 55000

@profile_time
def create_collection():
    connection = Connect.get_connection()
    db = connection['Lab4']
    # db.drop_collection('ZNO_result')
    collection = db['ZNO_result']
    print("IN_DB - ", collection.count_documents({}))
    print('='*100)
    return collection

@profile_time
def data_to_DB(collection):
    for year in [2019,2020]:
        print(str(year) + ' file')
        print("-"*100)
        max_row_number = step
        with open(NamePath_files.format(year=year), encoding='cp1251') as f:
            header = []
            HEADER_CREATING_FLG = False
            data = csv.reader(f,delimiter=';',quotechar='"',quoting=csv.QUOTE_ALL)   
            i = 1            
            IN_DB_ROWS = collection.count_documents({"year" : year})
            if IN_DB_ROWS:
                max_row_number = IN_DB_ROWS + step
            
            for row in data:
                if IN_DB_ROWS < i and HEADER_CREATING_FLG:
                    if i <= Max_Amount_To_Upload:
                        row.append(year)
                        # i, max_row_number = line_to_dict_list(header, row, i, step, max_row_number,year, collection)
                    else:
                        break
                else:
                    if IN_DB_ROWS == i:
                        print(i, 'line is already in DB')
                        print("-"*100)
                        time.sleep(3)
                    i += 1
                if header == []:
                    header = row
                    header.append("year")
                    HEADER_CREATING_FLG = True
        print('End {yearFile} file to DB\nIn Collection {lines} lines'.format(yearFile = year,lines = collection.count_documents({})))
        print('='*100)

def line_to_dict_list(header, row, i, step, max_row_number,year, collection):
    print(year,' <> ', i)
    i += 1
    line_dict = { h_value : "-" for h_value in header }
    for j in range(len(header)):
        if clean_csv_value(row[j]) == row[j]:
            line_dict[header[j]] = row[j]
        else:
            line_dict[header[j]] = clean_csv_value(row[j])
            
    collection.insert_one(line_dict)
        
    if max_row_number < i:
        print("-"*100)
        max_row_number += step
        time.sleep(2)

    return i, max_row_number

def clean_csv_value(value):
    if value == 'null':
        return value
    try:
        res = float(value.replace(',', '.'))
        return res
    except:
        return value
    
@profile_time
def execute_query(collection):
    header = ["Year", "Max English Results"]
    
    with open('results/result_query.csv', 'w') as res_file:
        result_writer = csv.writer(res_file, delimiter=';')
        result_writer.writerow(header)
        for result in collection.aggregate([{"$match" : { "engTestStatus" :  'Зараховано' }},{"$group" : { "_id" : "$year","EngMaxResults": { "$max": "$engBall100" }}},{"$sort": SON([("EngMaxResults", -1), ("_id", -1)])}]):
            result_writer.writerow(list(dict(result).values()))
        

if __name__ == '__main__':
    collection = create_collection()
    
    data_to_DB(collection)
    
    execute_query(collection)