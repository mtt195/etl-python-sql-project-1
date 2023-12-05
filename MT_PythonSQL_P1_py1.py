# -------------------------------------------------- #
# AUTHOR NAME: MACIEJ TOMASZEWSKI                    #
# CREATE DATE: 02.12.2023                            #
# DESCRIPTION:                                       #
#  MY FIRST PYTHON & SQL PROJECT. THIS SCRIPT IS     #
#  USED TO PROCESS SAMPLE, FICTIOUS DATA USING       #
#  PYTHON: EXTRACT FROM FLAT FILES, TRANSFORM -      #
#  COMBINE WITH EXISTING DATA, AND FINALLY LOAD      #
#  TO DEDICATED TABLES FOR DATA & ANALYZES.          #
#  THIS IS THE ONLY PYTHON SCRIPT USED.              #

#                PYTHON SCRIPT 1/1                   #
# -------------------------------------------------- #



##################################
try:
##################################
    # 0. PROCESS STARTS
    v_step, v_exec_status = '0. PROCESS STARTS', 'ERROR'
    print(v_step, '\n')



    # 1. IMPORT NECESSARY LIBRARIES
    from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Float, Boolean
    from sqlalchemy import inspect, Select, func, Insert, literal_column, Case, between
    import pandas as pd

    v_step = '1. LIBRARIES IMPORTED'
    print(v_step, '\n')



    # 2. SET VARIABLES
    v_db_name = 'MT_PythonSQL_Project1'
    v_engine = create_engine('mssql://@localhost/' + v_db_name + '?driver=odbc+driver+17+for+sql+server')
    v_mdata = MetaData()

    v_user_email = 'YourName.YourSurname@CorpName.com' # your name and surname/email
    v_cust_flat_file_data_path = r'C:\Users\tomas\Desktop\Python\Projects\ETL 1\etl1_customers.txt' # location of flat file with CUSTOMERS data
    v_sls_flat_file_data_path = r'C:\Users\tomas\Desktop\Python\Projects\ETL 1\etl1_sales.csv' # location of flat file with SALES data

    v_connection = v_engine.connect()
    v_connection.begin()

    v_step = '2. VARIABLES SET'
    print(v_step, '\n')



    # ETL TABLES: CUSTOMERS, SALES
    # ############################
    v_step = 'ETL TABLES'
    print(v_step)

    v_customers_s, v_sales_s = 'Customers_stage', 'Sales_stage'



    # 3. CREATE DATA STRUCTURES
    t_customers_s = Table(
        v_customers_s, v_mdata
        , Column('CustomerID', Integer), Column('CustomerName', String(50))
        , Column('City', String(50)), Column('PostalCode', Integer), Column('Region', String(10))
    )

    t_sales_s = Table(
        v_sales_s, v_mdata 
        , Column('OrderDate', String(50)), Column('ShipDate', String(50)), Column('CustomerID', Integer)
        , Column('City', String(50)), Column('PostalCode', Integer), Column('Region', String(20))
        , Column('ItemID', Integer), Column('Category', String(50)), Column('ItemName', String(100))
        , Column('Sales', Float), Column('Quantity', Integer), Column('Profit', Float)
    )

    v_mdata.create_all(v_engine, tables = [t_customers_s, t_sales_s])

    v_step = '3. DATA STRUCTURES CREATED'
    print(v_step, '\n')



    # 4. VIEW TABLES
    lst_tables = inspect(v_engine).get_table_names()
    print('4. TABLES LIST:')
    for t in lst_tables:
        print(t)
    print('\n')

    v_step = '4. TABLES NAMES DISPLAYED'
    print(v_step, '\n')



    # 5. INSERT DATA

    # 5.1. TO STAGE TABLES
    v_customers_data = pd.read_csv(v_cust_flat_file_data_path, sep = '\t', header = 0)
    v_customers_data.to_sql(v_customers_s, con = v_engine, index = False, if_exists = 'append')

    v_sales_data = pd.read_csv(v_sls_flat_file_data_path, sep = ';', header = 0)
    v_sales_data.to_sql(v_sales_s, con = v_engine, index = False, if_exists = 'append')

    v_step = '5.1. STAGE TABLES INSERTED'
    print(v_step, '\n')


    # 5.2. TO TARGET TABLES
    v_customers, v_sales,  = 'Customers', 'Sales'

    t_customers_s = Table(v_customers_s, v_mdata, autoload_with = v_engine)
    t_customers = Table(v_customers, v_mdata, autoload_with = v_engine)

    t_sales_s = Table(v_sales_s, v_mdata, autoload_with = v_engine)
    t_sales = Table(v_sales, v_mdata, autoload_with = v_engine)


    # 5.2.1. CUSTOMERS
    v_customers_columns_to_insert = [
        t_customers.columns.DB_CreateTimestamp, t_customers.columns.DB_CreateUser
        , t_customers.columns.CustomerID,       t_customers.columns.CustomerName
        , t_customers.columns.City,             t_customers.columns.PostalCode
        , t_customers.columns.Region,           t_customers.columns.IsValid
    ]

    v_select_customers_s = Select(
        func.now().label('DB_CreateTimestamp')
        ,literal_column("'" + v_user_email + "'").label('DB_CreateUser')
        ,t_customers_s.columns.CustomerID
        ,t_customers_s.columns.CustomerName
        ,t_customers_s.columns.City
        ,t_customers_s.columns.PostalCode
        ,t_customers_s.columns.Region
        ,literal_column('1').label('IsValid')
    )

    v_insert_customers = Insert(t_customers).from_select(v_customers_columns_to_insert, v_select_customers_s)
    v_connection.execute(v_insert_customers)


    # 5.2.2. SALES
    v_sales_columns_to_insert = [
        t_sales.columns.DB_CreateTimestamp, t_sales.columns.DB_CreateUser
        , t_sales.columns.OrderDate,        t_sales.columns.ShipDate
        , t_sales.columns.CustomerID,       t_sales.columns.City
        , t_sales.columns.PostalCode,       t_sales.columns.Region
        , t_sales.columns.ItemID,           t_sales.columns.Category
        , t_sales.columns.ItemName,         t_sales.columns.Sales
        , t_sales.columns.Quantity,         t_sales.columns.Profit
    ]

    v_select_sales_s = Select(
        func.now().label('DB_CreateTimestamp')
        ,literal_column("'" + v_user_email + "'").label('DB_CreateUser')
        ,func.concat(func.right(t_sales_s.columns.OrderDate, 4), '-', func.substring(t_sales_s.columns.OrderDate, 4, 2), '-', func.left(t_sales_s.columns.OrderDate, 2)).label('OrderDate')
        ,func.concat(func.right(t_sales_s.columns.ShipDate, 4), '-', func.substring(t_sales_s.columns.ShipDate, 4, 2), '-', func.left(t_sales_s.columns.ShipDate, 2)).label('ShipDate')
        ,t_sales_s.columns.CustomerID
        ,t_sales_s.columns.City
        ,t_sales_s.columns.PostalCode
        ,t_sales_s.columns.Region
        ,t_sales_s.columns.ItemID
        ,t_sales_s.columns.Category
        ,t_sales_s.columns.ItemName
        ,t_sales_s.columns.Sales
        ,t_sales_s.columns.Quantity
        ,t_sales_s.columns.Profit
    )

    v_insert_sales = Insert(t_sales).from_select(v_sales_columns_to_insert, v_select_sales_s)
    v_connection.execute(v_insert_sales)

    v_step = '5.2. TARGET TABLES INSERTED'
    print(v_step, '\n')



    # 6. DROP STAGE TABLES
    t_customers_s.drop(bind = v_engine)
    t_sales_s.drop(bind = v_engine)

    v_step = '6. STAGE TABLES DROPPED'
    print(v_step, '\n')



    # TARGET TABLES FOR DATA ANALYZES: CUST_ANALYTICS, ITEMS_ANALYTICS
    # ################################################################
    v_step = 'TARGET TABLES FOR DATA ANALYZES'
    print(v_step)

    v_cust_analytics_s, v_items_analytics_s = 'CustAnalytics_stage', 'ItemsAnalytics_stage'



    # 7. CREATE DATA STRUCTURES
    t_cust_analytics_s = Table(
        v_cust_analytics_s, v_mdata
        , Column('CustomerID', Integer), Column('CustomerName', String(50)), Column('Region', String(10)), Column('IsVIP', Boolean)               
    )

    t_items_analytics_s = Table(
        v_items_analytics_s, v_mdata
        , Column('ItemID', Integer), Column('ItemName', String(100)), Column('Category', String(50))
        , Column('IsPremium', Boolean), Column('TotalSales', Float), Column('TotalProfit', Float)
    )

    v_mdata.create_all(bind=v_engine, tables = [t_cust_analytics_s, t_items_analytics_s])

    v_step = '7. STAGE TABLES CREATED'
    print(v_step, '\n')



    # 8. VIEW TABLES
    lst_tables = inspect(v_engine).get_table_names()
    print('8. TABLES LIST:')
    for t in lst_tables:
        print(t)
    print('\n')

    v_step = '8. TABLES NAMES DISPLAYED'
    print(v_step, '\n')



    # 9. INSERT DATA
    v_cust_analytics_s, v_cust_analytics = 'CustAnalytics_stage', 'CustAnalytics'
    v_items_analytics_s, v_items_analytics = 'ItemsAnalytics_stage', 'ItemsAnalytics'
    v_custvip, v_itemsp = 'CustVIP', 'ItemsP' 


    # 9.1. TO STAGE TABLES

    # 9.1.1. CUST_ANALYTICS_s
    ca_col_lst_s = [col for col in t_cust_analytics_s.columns.keys()]

    t_custvip = Table(v_custvip, v_mdata, autoload_with = v_engine)
    q_with_CustVIP = Select(t_custvip.c.CustomerID).where(t_custvip.c.IsValid == 1).cte('_cte_CustVIP')

    q_customers = Select(
        t_customers.c.CustomerID.label('CustID')
        ,t_customers.c.CustomerName.label('CustNm')
        ,t_customers.c.Region.label('Reg')
        ,Case((q_with_CustVIP.c.CustomerID != None, 1), else_ = 0).label('IsVip')
    ).join(q_with_CustVIP, q_with_CustVIP.c.CustomerID == t_customers.c.CustomerID, isouter = True)
    q_customers = q_customers.where(t_customers.c.IsValid == 1)
    q_customers = q_customers.order_by(t_customers.c.CustomerName)

    q_insert_cust_analytics_s = Insert(t_cust_analytics_s).from_select(ca_col_lst_s, q_customers)
    v_connection.execute(q_insert_cust_analytics_s) # insert


    # 9.1.2. ITEMS_ANALYTICS_S
    ia_col_lst_s = [col for col in t_items_analytics_s.columns.keys()]

    t_itemsp = Table(v_itemsp, v_mdata, autoload_with = v_engine)
    q_with_ItemsP = Select(t_itemsp.c.ItemID).cte('_cte_itemsp')

    q_sales = Select(
        t_sales.c.ItemID,
        t_sales.c.ItemName,
        t_sales.c.Category,
        Case((q_with_ItemsP.c.ItemID != None, 1), else_ = 0).label('IsPremium'),
        func.sum(t_sales.c.Sales).label('TotalSales'),
        func.sum(t_sales.c.Profit).label('TotalProfit')
    ).join(q_with_ItemsP, q_with_ItemsP.c.ItemID == t_sales.c.ItemID, isouter = True)
    q_sales = q_sales.where(between(func.year(t_sales.c.OrderDate), 2015, 2017))
    q_sales = q_sales.group_by(t_sales.c.ItemID, t_sales.c.ItemName, t_sales.c.Category, q_with_ItemsP.c.ItemID, Case((q_with_ItemsP.c.ItemID != None, 1), else_=0))
    q_sales = q_sales.order_by(t_sales.c.Category, t_sales.c.ItemID)

    q_insert_items_analytics_s = Insert(t_items_analytics_s).from_select(ia_col_lst_s, q_sales)
    v_connection.execute(q_insert_items_analytics_s) # insert

    v_step = '9.1. STAGE TABLES INSERTED'
    print(v_step, '\n')


    # 9.2. TO TARGET TABLES

    # 9.2.1. CUST_ANALYTICS
    t_cust_analytics = Table(v_cust_analytics, v_mdata, autoload_with = v_engine)
    ca_col_lst = [col for col in t_cust_analytics.columns.keys() if col != 'DB_RowID']

    q_cust_analytics_s = Select(
        func.now().label('DB_CreateTimestamp')
        ,literal_column("'" + v_user_email + "'").label('DB_CreateUser')
        ,t_cust_analytics_s.c.CustomerID
        ,t_cust_analytics_s.c.CustomerName
        ,t_cust_analytics_s.c.Region
        ,t_cust_analytics_s.c.IsVIP
    )

    q_insert_cust_analytics = Insert(t_cust_analytics).from_select(ca_col_lst, q_cust_analytics_s)
    v_connection.execute(q_insert_cust_analytics)


    # 9.2.2. ITEMS_ANALYTICS
    t_items_analytics = Table(v_items_analytics, v_mdata, autoload_with = v_engine)
    ia_col_lst = [col for col in t_items_analytics.columns.keys() if col != 'DB_RowID']

    q_items_analytics_s = Select(
        func.now().label('DB_CreateTimestamp')
        ,literal_column("'" + v_user_email + "'").label('DB_CreateUser')
        ,t_items_analytics_s.c.ItemID
        ,t_items_analytics_s.c.ItemName
        ,t_items_analytics_s.c.Category
        ,t_items_analytics_s.c.IsPremium
        ,t_items_analytics_s.c.TotalSales
        ,t_items_analytics_s.c.TotalProfit
    )

    q_insert_items_analytics = Insert(t_items_analytics).from_select(ia_col_lst, q_items_analytics_s)
    v_connection.execute(q_insert_items_analytics)

    v_connection.commit()
    v_connection.close()

    v_step = '9.2. TARGET TABLES INSERTED'
    print(v_step, '\n')



    # 10. DROP STAGE TABLES
    ##################################################
    t_items_analytics_s.drop(bind = v_engine)
    t_cust_analytics_s.drop(bind = v_engine)

    v_step = '10. STAGE TABLES DROPPED'
    print(v_step, '\n')

    v_exec_status = 'SUCCESS'



##################################
except Exception as e:
##################################
    if (v_step != '0. PROCESS STARTS' and v_step != '1. LIBRARIES IMPORTED' and v_step != '2. VARIABLES SET'):
        v_connection.rollback()
        v_connection.close()

    print('PROCESS STOPPED:')
    print(f'Error occured in step [{v_step}]: {type(e).__name__} - {e}')



##################################
finally:
##################################    
    print(f'\nProcess completed with status: {v_exec_status}.')
