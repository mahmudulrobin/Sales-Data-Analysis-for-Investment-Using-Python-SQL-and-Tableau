
import pandas as pd
import datetime
import sqlite3
from sqlite3 import Error
import os


def create_connection(db_file, delete_db=False):
    import os
    if delete_db and os.path.exists(db_file):
        os.remove(db_file)

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql, drop_table_name=None):

    if drop_table_name:
        try:
            c = conn.cursor()
            c.execute("""DROP TABLE IF EXISTS %s""" % (drop_table_name))
        except Error as e:
            print(e)

    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def execute_sql_statement(sql_statement, conn):
    cur = conn.cursor()
    cur.execute(sql_statement)

    rows = cur.fetchall()

    return rows


def step1_create_region_table(data_filename, normalized_database_filename):

    db_file = open(data_filename, 'r')
    lines = db_file.readlines()
    data = lines[1:]
    db_file.close()

    regions = set()

    for line in data:
        region = line.split('\t')[4]
        regions.add(region)

    region_list = list(regions)
    region_list.sort()

    query = """CREATE TABLE Region(RegionID INTEGER,Region TEXT);"""

    conn = create_connection(normalized_database_filename)
    create_table(conn, query, 'Region')

    sql = """INSERT INTO Region(RegionID, Region) VALUES """
    for i in range(1, len(region_list)+1):
        sql += f"({i}, '{region_list[i-1]}')"
        if i != len(region_list):
            sql += ", "
        else:
            sql += ";"

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()


def step2_create_region_to_regionid_dictionary(normalized_database_filename):

    query = '''
		SELECT * FROM Region;
	'''

    conn = create_connection(normalized_database_filename)
    rows = execute_sql_statement(query, conn)

    dictionary = {}
    for ind, reg in rows:
        dictionary[reg] = ind

    return dictionary


def step3_create_country_table(data_filename, normalized_database_filename):

    db_file = open(data_filename, 'r')
    lines = db_file.readlines()
    data = lines[1:]
    db_file.close()

    dictionary = step2_create_region_to_regionid_dictionary(
        normalized_database_filename)

    countries = []
    sql_countries = []

    for line in data:
        region = line.split('\t')[4]
        country = line.split('\t')[3]
        if region in dictionary.keys() and country not in countries:
            countries.append(country)
            sql_countries.append([country, dictionary[region]])

    sql_countries.sort(key=lambda x: x[0])

    query = '''CREATE TABLE Country(CountryID INTEGER, Country TEXT, RegionID INTEGER);'''

    conn = create_connection(normalized_database_filename)
    create_table(conn, query, 'Country')

    sql = '''INSERT INTO Country(CountryID, Country, RegionID) VALUES '''
    for i in range(len(sql_countries)):
        info = sql_countries[i]
        sql += f"({i+1}, '{info[0]}', {info[1]})"
        if i != len(sql_countries)-1:
            sql += ","
        else:
            sql += ";"

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()


def step4_create_country_to_countryid_dictionary(normalized_database_filename):

    query = '''
		SELECT * FROM Country;
	'''

    conn = create_connection(normalized_database_filename)
    rows = execute_sql_statement(query, conn)

    dictionary = {}
    for ind, con, f_id in rows:
        dictionary[con] = ind

    return dictionary


def step5_create_customer_table(data_filename, normalized_database_filename):

    db_file = open(data_filename, 'r')
    lines = db_file.readlines()
    data = lines[1:]
    db_file.close()

    dictionary = step4_create_country_to_countryid_dictionary(
        normalized_database_filename)

    customers = []

    i = 1
    for line in data:
        country = line.split('\t')[3]
        name = line.split('\t')[0]
        fname = name.split(' ')[0]

        lname = ' '.join(name.split(' ')[1:])

        address = line.split('\t')[1]
        city = line.split('\t')[2]

        if country in dictionary.keys():
            customers.append(
                [fname, lname, address, city, dictionary[country]])

    customers.sort(key=lambda x: ' '.join([x[0], x[1]]))

    query = '''CREATE TABLE Customer (CustomerID integer, FirstName Text, LastName Text, Address Text, City Text,CountryID integer);'''

    conn = create_connection(normalized_database_filename)
    create_table(conn, query, 'Customer')

    sql = '''INSERT INTO Customer (CustomerID, FirstName, LastName, Address, City, CountryID) VALUES '''
    for i in range(len(customers)):
        cstm = customers[i]

        sql += '(' + str(i+1) + ','
        sql += "'" + cstm[0] + "',"
        sql += "'" + cstm[1] + "',"
        if ("," in cstm[2]) or ("'" in cstm[2]):
            sql += '"' + cstm[2] + '",'
        else:
            sql += "'" + cstm[2] + "',"
        sql += "'" + cstm[3] + "',"
        sql += str(cstm[4]) + ')'

        if i != len(customers)-1:
            sql += ","
        else:
            sql += ";"

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()


def step6_create_customer_to_customerid_dictionary(normalized_database_filename):

    query = '''
		SELECT * FROM Customer;
	'''
    conn = create_connection(normalized_database_filename)
    rows = execute_sql_statement(query, conn)

    dictionary = {}
    for ind, fname, lname, add, ct, c_id in rows:
        dictionary[f'{fname} {lname}'] = ind

    return dictionary


def step7_create_productcategory_table(data_filename, normalized_database_filename):

    db_file = open(data_filename, 'r')
    lines = db_file.readlines()
    data = lines[1:]
    db_file.close()

    categories_unq = []
    products_unq = []

    for line in data:
        categories = line.split('\t')[6]
        descriptions = line.split('\t')[7]

        categories_list = categories.split(";")
        descriptions_list = descriptions.split(";")

        for i in range(len(categories_list)):
            if categories_list[i] not in categories_unq:
                categories_unq.append(categories_list[i])
                products_unq.append([categories_list[i], descriptions_list[i]])

    products_unq.sort(key=lambda x: x[0])

    query = '''CREATE TABLE ProductCategory(ProductCategoryID INTEGER, ProductCategory TEXT, ProductCategoryDescription TEXT);'''

    conn = create_connection(normalized_database_filename)
    create_table(conn, query, 'ProductCategory')

    sql = '''INSERT INTO ProductCategory(ProductCategoryID, ProductCategory, ProductCategoryDescription) VALUES '''

    for i in range(len(products_unq)):
        prdct = products_unq[i]
        sql += '(' + str(i+1) + ','
        sql += "'" + prdct[0] + "',"
        if ',' in prdct[1]:
            sql += '"' + prdct[1] + '")'
        else:
            sql += "'" + prdct[1] + "')"

        if i != len(products_unq)-1:
            sql += ","
        else:
            sql += ";"

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()


def step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename):

    query = '''SELECT * FROM ProductCategory;'''

    conn = create_connection(normalized_database_filename)
    rows = execute_sql_statement(query, conn)

    dictionary = {}
    for ind, cat, des in rows:
        dictionary[cat] = ind

    return dictionary


def step9_create_product_table(data_filename, normalized_database_filename):

    db_file = open(data_filename, 'r')
    lines = db_file.readlines()
    data = lines[1:]
    db_file.close()

    dictionary = step8_create_productcategory_to_productcategoryid_dictionary(
        normalized_database_filename)

    products_unq = []
    unq_data = []

    for line in data:

        categories = line.split('\t')[6]
        pnames = line.split('\t')[5]
        pprices = line.split('\t')[8]

        categories_list = categories.split(";")
        pnames_list = pnames.split(";")
        pprices_list = pprices.split(";")

        for i in range(len(pnames_list)):
            name = pnames_list[i]
            if (name not in products_unq) and (categories_list[i] in dictionary.keys()):
                products_unq.append(name)
                unq_data.append(
                    [name, pprices_list[i], dictionary[categories_list[i]]])

    unq_data.sort(key=lambda x: x[0])

    query = '''CREATE TABLE Product (ProductID integer, ProductName Text, ProductUnitPrice Real, ProductCategoryID integer);'''

    conn = create_connection(normalized_database_filename)
    create_table(conn, query, 'Product')

    sql = '''INSERT INTO Product(ProductID, ProductName, ProductUnitPrice, ProductCategoryID) VALUES '''

    for i in range(len(unq_data)):
        data = unq_data[i]
        sql += '(' + str(i+1) + ','
        sql += '"' + data[0] + '",'
        sql += str(data[1]) + ','
        sql += str(data[2]) + ')'

        if i != len(unq_data)-1:
            sql += ","
        else:
            sql += ";"

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()


def step10_create_product_to_productid_dictionary(normalized_database_filename):

    query = '''SELECT * FROM Product;'''

    conn = create_connection(normalized_database_filename)
    rows = execute_sql_statement(query, conn)

    dictionary = {}
    for ind, name, price, c_id in rows:
        dictionary[name] = ind

    return dictionary


def step11_create_orderdetail_table(data_filename, normalized_database_filename):

    db_file = open(data_filename, 'r')
    lines = db_file.readlines()
    data = lines[1:]
    db_file.close()

    products_dict = step10_create_product_to_productid_dictionary(
        normalized_database_filename)
    customer_dict = step6_create_customer_to_customerid_dictionary(
        normalized_database_filename)

    order_list = []

    for line in data:
        name = line.split('\t')[0]

        pnames = line.split('\t')[5]
        pnames_list = pnames.split(";")

        order_dates = line.split('\t')[10].strip()
        order_date_list = order_dates.split(";")

        order_date_list_ = [datetime.datetime.strptime(order_date, '%Y%m%d').strftime(
            '%Y-%m-%d') for order_date in order_date_list]

        quantities = line.split('\t')[9]

        quantities_list = quantities.split(";")

        quantities_list_ = [int(quantity) for quantity in quantities_list]

        for i in range(len(pnames_list)):
            pname = pnames_list[i]
            if (pname in products_dict.keys()) and (name in customer_dict.keys()):
                order_list.append([customer_dict[name], products_dict[pname], order_date_list_[
                                  i], quantities_list_[i]])

    query = '''CREATE TABLE OrderDetail (OrderID INTEGER, CustomerID INTEGER, ProductID INTEGER, OrderDate DATE, QuantityOrdered INTEGER);'''

    conn = create_connection(normalized_database_filename)
    create_table(conn, query, 'OrderDetail')

    sql = '''INSERT INTO OrderDetail(OrderID, CustomerID, ProductID, OrderDate, QuantityOrdered) VALUES '''

    for i in range(len(order_list)):
        order = order_list[i]
        sql += '(' + str(i+1) + ','
        sql += str(order[0]) + ','
        sql += str(order[1]) + ','
        sql += "'" + str(order[2]) + "',"
        sql += str(order[3]) + ')'

        if i != len(order_list)-1:
            sql += ","
        else:
            sql += ";"

    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()


def customer_order(conn, CustomerName):

    query = '''SELECT * FROM Customer;'''
    rows = execute_sql_statement(query, conn)

    customer_dict = {}
    for ind, fname, lname, add, ct, c_id in rows:
        customer_dict[f'{fname} {lname}'] = ind

    cust_id = customer_dict[CustomerName]

    sql_statement = f"""SELECT c.FirstName || ' ' || c.LastName as Name, b.ProductName, a.OrderDate, b.ProductUnitPrice, a.QuantityOrdered, ROUND((b.ProductUnitPrice * a.QuantityOrdered), 2) as Total  FROM OrderDetail a INNER JOIN Product b on b.ProductID = a.ProductID INNER JOIN Customer c on c.CustomerID = a.CustomerID WHERE c.CustomerID = {cust_id};"""

    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def customer_lifetime_spendings(conn, CustomerName):

    query = '''SELECT * FROM Customer;'''
    rows = execute_sql_statement(query, conn)

    customer_dict = {}
    for ind, fname, lname, add, ct, c_id in rows:
        customer_dict[f'{fname} {lname}'] = ind

    cust_id = customer_dict[CustomerName]

    sql_statement = f"""SELECT c.FirstName || ' ' || c.LastName as Name, ROUND(SUM(b.ProductUnitPrice * a.QuantityOrdered), 2) as Total  FROM OrderDetail a INNER JOIN Product b on b.ProductID = a.ProductID INNER JOIN Customer c on c.CustomerID = a.CustomerID WHERE c.CustomerID = {cust_id};"""

    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def all_customer_lifetime_spendings(conn):

    sql_statement = """SELECT c.FirstName || ' ' || c.LastName as Name, ROUND(SUM(b.ProductUnitPrice * a.QuantityOrdered), 2) as Total  FROM OrderDetail a INNER JOIN Product b on b.ProductID = a.ProductID INNER JOIN Customer c on c.CustomerID = a.CustomerID GROUP BY Name ORDER BY Total DESC;"""

    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def region_wise_spendings(conn):

    sql_statement = """SELECT e.Region, ROUND(SUM(b.ProductUnitPrice * a.QuantityOrdered), 2) as Total  FROM OrderDetail a
	INNER JOIN Product b on b.ProductID = a.ProductID
	INNER JOIN Customer c on c.CustomerID = a.CustomerID
	INNER JOIN Country d on d.CountryID = c.CountryID
	INNER JOIN Region e on e.RegionID = d.RegionID
	GROUP BY e.Region
	ORDER BY Total DESC;"""

    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def country_wise_spendings(conn):

    sql_statement = """SELECT d.Country, ROUND(SUM(b.ProductUnitPrice * a.QuantityOrdered)) as CountryTotal  FROM OrderDetail a
	INNER JOIN Product b on b.ProductID = a.ProductID
	INNER JOIN Customer c on c.CustomerID = a.CustomerID
	INNER JOIN Country d on d.CountryID = c.CountryID
	GROUP BY d.Country
	ORDER BY CountryTotal DESC;"""

    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def region_wise_rank_countries(conn):

    sql_statement = """SELECT e.Region, d.Country, ROUND(SUM(b.ProductUnitPrice * a.QuantityOrdered)) as CountryTotal, RANK() OVER (PARTITION BY e.Region ORDER BY ROUND(SUM(b.ProductUnitPrice * a.QuantityOrdered)) DESC) as CountryRegionalRank FROM OrderDetail a
	INNER JOIN Product b on b.ProductID = a.ProductID
	INNER JOIN Customer c on c.CustomerID = a.CustomerID
	INNER JOIN Country d on d.CountryID = c.CountryID
	INNER JOIN Region e on e.RegionID = d.RegionID
	GROUP BY d.Country
	ORDER BY e.Region;"""

    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def region_wise_country_rank_1(conn):

    sql_statement = """WITH CRE AS
	(SELECT e.Region, d.Country, ROUND(SUM(b.ProductUnitPrice * a.QuantityOrdered)) as CountryTotal, RANK() OVER (PARTITION BY e.Region ORDER BY ROUND(SUM(b.ProductUnitPrice * a.QuantityOrdered)) DESC) as CountryRegionalRank FROM OrderDetail a
	INNER JOIN Product b on b.ProductID = a.ProductID
	INNER JOIN Customer c on c.CustomerID = a.CustomerID
	INNER JOIN Country d on d.CountryID = c.CountryID
	INNER JOIN Region e on e.RegionID = d.RegionID
	GROUP BY d.Country
	ORDER BY e.Region)

	SELECT * FROM CRE WHERE CountryRegionalRank = 1;"""

    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def Quarter_wise_spendings_customer(conn):

    sql_statement = """WITH Temp AS
	(SELECT 
	CASE 
  	WHEN cast(strftime('%m', OrderDate) as integer) BETWEEN 1 AND 3 THEN 'Q' || 1
  	WHEN cast(strftime('%m', OrderDate) as integer) BETWEEN 4 and 6 THEN 'Q' || 2
  	WHEN cast(strftime('%m', OrderDate) as integer) BETWEEN 7 and 9 THEN 'Q' || 3
  	ELSE 'Q' || 4 END as Quarter, 
	CAST((strftime('%Y', a.OrderDate)) as integer) AS Year, a.CustomerID, ROUND(SUM(b.ProductUnitPrice * a.QuantityOrdered)) as Total FROM OrderDetail a
	INNER JOIN Product b on b.ProductID = a.ProductID
	GROUP BY Quarter, Year, a.CustomerID
	ORDER BY Year, Quarter, a.CustomerID)

	SELECT * FROM Temp;"""

    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def quarter_wise_spending_with_quarter_wise_rank_customer(conn):

    sql_statement = """WITH Temp1 AS
	(SELECT 
	CASE 
	WHEN cast(strftime('%m', a.OrderDate) as integer) BETWEEN 1 AND 3 THEN 'Q' || 1
	WHEN cast(strftime('%m', a.OrderDate) as integer) BETWEEN 4 and 6 THEN 'Q' || 2
	WHEN cast(strftime('%m', a.OrderDate) as integer) BETWEEN 7 and 9 THEN 'Q' || 3
	ELSE 'Q' || 4 END as Quarter,
	CAST((strftime('%Y', a.OrderDate)) as integer) AS Year, a.CustomerID, ROUND(SUM(b.ProductUnitPrice * a.QuantityOrdered)) as Total
	FROM OrderDetail a INNER JOIN Product b on b.ProductID = a.ProductID
	GROUP BY Quarter, Year, a.CustomerID
	ORDER BY Year, Quarter, a.CustomerID),

	Temp2 AS
	(SELECT *,
	RANK() OVER(PARTITION BY Quarter, Year 
	ORDER BY Total DESC) AS CustomerRank
	FROM Temp1
	ORDER BY Year, Quarter)

	SELECT * FROM Temp2 WHERE CustomerRank < 6;"""

    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def month_wise_total_rank(conn):

    sql_statement = """WITH Temp AS
	(SELECT
	CASE cast(strftime('%m', a.OrderDate) as integer)
	WHEN 1 THEN 'January'
	WHEN 2 THEN 'February'
	WHEN 3 THEN 'March'
	WHEN 4 THEN 'April'
	WHEN 5 THEN 'May'
	WHEN 6 THEN 'June'
	WHEN 7 THEN 'July'
	WHEN 8 THEN 'August'
	WHEN 9 THEN 'September'
	WHEN 10 THEN 'October'
	WHEN 11 THEN 'November'
	WHEN 12 THEN 'December'
	END AS Month, 
	SUM(ROUND(b.ProductUnitPrice * a.QuantityOrdered)) as Total
	FROM OrderDetail a INNER JOIN Product b on b.ProductID = a.ProductID
	GROUP BY Month)
	SELECT *, Rank() OVER(ORDER BY Total DESC) AS TotalRank FROM Temp;"""

    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def customer_maxDayWithoutOrder(conn):

    sql_statement = """
	With Temp1 AS
	(SELECT a.CustomerID, b.FirstName, b.LastName, c.Country, a.OrderDate, LAG(a.OrderDate) OVER(PARTITION BY a.CustomerID ORDER BY a.OrderDate) as PreviousOrderDate, (julianday(a.OrderDate) - julianday(LAG(a.OrderDate)
	OVER(PARTITION BY a.CustomerID ORDER BY a.OrderDate))) as difference
	FROM OrderDetail a INNER JOIN Customer b ON b.CustomerID = a.CustomerID
	INNER JOIN Country c on c.CountryID = b.CountryID),

	Temp2 AS
	(SELECT CustomerID, max(difference) AS MaxDaysWithoutOrder FROM Temp1
	group by CustomerID)

	SELECT DISTINCT a.CustomerID, a.FirstName, a.LastName, a.Country, a.OrderDate, a.PreviousOrderDate, b.MaxDaysWithoutOrder FROM 
	Temp1 a INNER JOIN Temp2 b on b.MaxDaysWithoutOrder = a.difference and b.CustomerID = a.CustomerID
	GROUP BY a.CustomerID
	ORDER BY b.MaxDaysWithoutOrder DESC;
	"""

    # df = pd.read_sql_query(sql_statement, conn)
    return sql_statement
