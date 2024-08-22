import os
import sys
import logging
import cx_Oracle
from time import time
from string import Template
import snowflake.connector as sfc
from logging.handlers import RotatingFileHandler

#Log File properties
log_file = r"C:\Users\spalani5\Desktop\Automate_Manual_Testing\log.txt"
log_format ="%(asctime)s: %(levelname)s : %(message)s"

#Setting up log file here
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(log_format)
file_handler = RotatingFileHandler(log_file,maxBytes=10485760,backupCount=20)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)



def parse_table():
    try:
        with open(r"C:\Users\spalani5\Desktop\Automate_Manual_Testing\Input\Input_file.txt","r") as tble_file:
            tble_name = tble_file.readlines()
            
            for tbl in tble_name:
                table_name = tbl.rstrip('\n')
                oracle_data_count(table_name)                

    except Exception as e:
        print(e)

    
def oracle_data_count(table_name):      

    func_rtn = table_name

    orcl_con = cx_Oracle.connect('spalani5/rmsjcp1@ltb3-scan.jcpenney.com:1521/MOMSODS')
    snf_con = sfc.connect(user='spalani5', password='Tcs@2014', account='jcpenney.us-east-1', warehouse = 'jcp_supply_compute', database='jcpstg', schema='rms', role = 'jcpstg_etl_dvlpr_role')
    
    try:
        def_query = "SELECT COUNT(1) FROM tbl_name"
        complete_query = def_query.replace('tbl_name',str(func_rtn))
        orcl_cursor = orcl_con.cursor()
        snf_cursor = snf_con.cursor()
        orcl_in = time()
        orcl_count = orcl_cursor.execute(complete_query).fetchall()
        orcl_out = time()
        orcl_exec_time = orcl_out - orcl_in
        #print(orcl_exec_time)
        snf_in = time()
        snf_count = snf_cursor.execute(complete_query).fetchall()
        snf_out = time()
        snf_exec_time = snf_out - snf_in
        #print(orcl_exec_time)
        
        if orcl_count[0] == snf_count[0]:
            print( "@"*10+" "*10+"Count Validation - Pass"+" "*10+"@"*10+'\n'+"Table Name  :",str(func_rtn)+'\n'+"Oracle Count:",str(orcl_count[0][0])+'\n'+"snf Count      :",str(snf_count[0][0])+'\n')
            aggregation_form(table_name)
        else:
            print( "@"*10+" "*10+"Count Validation - Fail"+" "*10+"@"*10+'\n'+"Table Name  :",str(func_rtn)+'\n'+"Oracle Count:",str(orcl_count[0][0])+'\n'+"snf Count      :",str(snf_count[0][0])+'\n'+"~"*100)
            
            
    except Exception as e:     
        return(e)

    finally:
        orcl_con.close()
        snf_con.close()


def aggregation_form(table_name):

    t_name = table_name
    snf_con = sfc.connect(user='spalani5', password='Tcs@2014', account='jcpenney.us-east-1', warehouse = 'jcp_supply_compute', database='jcpstg', schema='rms', role = 'jcpstg_etl_dvlpr_role')
    try:
        sql_build = """
                                    SELECT 
                                        DISTINCT(COLUMN_NAME)
                                    FROM 
                                        INFORMATION_SCHEMA.COLUMNS 
                                    WHERE 
                                        TABLE_NAME= 'tbl_name' and
                                    DATA_TYPE  IN( 'NUMBER')
                                """
        
        complete_query = sql_build.replace('tbl_name',str(t_name))
        

        cursor = snf_con.cursor()        
        order_details = cursor.execute(complete_query).fetchall()
    
        query_list = []
        
        for val in order_details:            
            t = Template("SUM($sub),")
            l_var_sub2 = t.substitute(sub = val[0])            
            query_list.append(l_var_sub2)            
    
        result = ' '
        for val in query_list:            
            result += str(val)
        
        formulate_query  = "SELECT "+str(result)+"FROM"+str(t_name)
        cursor_query = formulate_query.replace(",FROM"," FROM ")
        aggregation_Comparision(cursor_query,query_list)

    except Exception as e:     
        return(e)

    finally:
        snf_con.close()

def aggregation_Comparision(cursor_query,query_list):
    exe_query = cursor_query
    col_info = query_list
    snf_con = sfc.connect(user='spalani5', password='Tcs@2014', account='jcpenney.us-east-1', warehouse = 'jcp_supply_compute', database='jcpstg', schema='rms', role = 'jcpstg_etl_dvlpr_role')
    orcl_con = cx_Oracle.connect('spalani5/rmsjcp1@ltb3-scan.jcpenney.com:1521/MOMSODS')
    try:
        snf_cursor = snf_con.cursor()
        snf_details = snf_cursor.execute(exe_query).fetchall()
        

        orcl_cursor = orcl_con.cursor()
        orcl_details = orcl_cursor.execute(exe_query).fetchall()
        
        snf_data = []
        for val in snf_details[0]:
            if not val:
                data = val
                snf_data.append(data)
            else:
                data = float(val)
                snf_data.append(data)
       
        
        orcl_data = []
        for val in orcl_details[0]:
            if not val:
                data = val
                orcl_data.append(data)
            else:   
                data = float(val)
                orcl_data.append(data)
     
        print("@"*10+" "*10+"aggregation_Comparision- Result"+" "*10+"@"*10+'\n'+" "*40+"Snowflake Vs Oracle"+" "*25 )
        
        for i in range(0,len(snf_data),1):
                    
            if  snf_data[i] == orcl_data[i]:
                print(col_info[i],snf_data[i],"==",orcl_data[i]," :: Pass")

            else:
                print(col_info[i],snf_data[i],"=!",orcl_data[i]," :: Fail")
    
        print("~"*100)    
    except Exception as e:     
        return(e)

    finally:
        snf_con.close()
        orcl_con.close()


        

if __name__ == "__main__":
    parse_table()
    #row_lvl_cmpr()
    #aggregation_Comparision(cursor_query,query_list)



    
    
    
    
    
    
