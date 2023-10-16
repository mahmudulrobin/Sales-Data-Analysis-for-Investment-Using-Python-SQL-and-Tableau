# Data Wrangling, Extract Information and Visualization from Sales Raw Data - Python, SQL, Tableau

## Summary
I worked with data that required data wrangling to retrieve information that was useful. created a relational database with tables employing several pieces of data, including nations, regions, clients, goods, and orders. Identify the top-selling items, nations, and regions for each category (for example, Country) based on the total quantity of sales. Finally, additional very interesting information has been provided using dashboard in Tableau.

## Data Preparation

Dataset link: https://drive.google.com/file/d/1QOeEOTbFID1PQGrxU4Blgpl7eSUYpLtz/view?usp=sharing

The file consists of 11 columns separated by tab and each row is a customer with all of the products they have ordered. Columns are Name, Address, City, Country, Region, ProductName, ProductCategory, ProductCategoryDescription, ProductUnitPrice, QuantityOrdered, OrderDate. Last six columns are linked and each value in a column is separated by semicolon. So, after spliting and zipping them up we get the one information from each column. Basically for each customer, we have multiple data for the last six columns.

## Tools
1. SQLlite3
2. Python
3. Tableau

## Part 1: Creating Tables
Steps:
1. Understand the data structure by analyzing few lines of data.
2. Create Table with necessary column names.
3. Prepare the data using split and removing duplicates and loop through each line of the data.
4. Assign an ID to each customer, country, region, product and order to use it for joining tables later.
5. Insert the information to the table by looping through each row of data from step 3.

Example:

Creating OrderDetail Table which has these informations: OrderID, CustomerID, ProductID, OrderDate, QuantityOrdered.

    # reading file and exclude the row with column names
    db_file = open(data_filename, 'r')
    lines = db_file.readlines()
    data = lines[1:]
    db_file.close()
    
    # fetching dictionary consisting ID of product and customer
    products_dict = step10_create_product_to_productid_dictionary(
        normalized_database_filename)
    customer_dict = step6_create_customer_to_customerid_dictionary(
        normalized_database_filename)

    order_list = []

    # preparing the data looping through each line
    for line in data:
        name = line.split('\t')[0]
       
        pnames = line.split('\t')[5]
        # spliting pnames again with ';' to get all product order information for single customer
        pnames_list = pnames.split(";")

        order_dates = line.split('\t')[10].strip()
        order_date_list = order_dates.split(";")

        # Changing the date format to '%Y-%m-%d'
        order_date_list_ = [datetime.datetime.strptime(order_date, '%Y%m%d').strftime(
            '%Y-%m-%d') for order_date in order_date_list]

        quantities = line.split('\t')[9]

        quantities_list = quantities.split(";")
        
        # Converting type of each quantity from str to int
        quantities_list_ = [int(quantity) for quantity in quantities_list]

        # adding order information for the current customer to the order_list which will be used to create INSERT query
        for i in range(len(pnames_list)):
            pname = pnames_list[i]
            if (pname in products_dict.keys()) and (name in customer_dict.keys()):
                order_list.append([customer_dict[name], products_dict[pname], order_date_list_[
                                  i], quantities_list_[i]])

    # query to create table
    query = '''CREATE TABLE OrderDetail (OrderID INTEGER, CustomerID INTEGER, ProductID INTEGER, OrderDate DATE, QuantityOrdered INTEGER);'''

    # creating the connection with the database
    conn = create_connection(normalized_database_filename)
    # creating table with the function create_table created before
    create_table(conn, query, 'OrderDetail')

    # preparing the insert query
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

    # executing the query and closing the connection
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()

## Part 2: Extracting useful Information

Steps:
1. Take a glance on each table to see if the information is in desired format to perform query.
2. Create and execute query to get desired information and see in SQLiteStudio if the information looks good.

(Had to use julianday() instead of DATEDIFF() to get the date difference)

Example:

Extract longest duration in between two consecutive orders for each customer.

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

## Part 3: Visualization in Tableau

Steps:
1. The top city, nation, product category, and product name are displayed.
2. Information at the country level has been given using a map. On hover over the nation, information at the city level is displayed. There is an option to expand the sales data by city if a country is chosen.
3. Product category sales have been displayed. When you hover over each product category, the top products in that category are displayed. Step 2's selection of a country will filter sales data by that nation's most popular product categories.
4. The last three years' worth of Discrete time series sales data have been given with quarter-level granularity. Additionally, it will be filtered by country level.

To view in Tableau Public and interact: [https://public.tableau.com/views/SalesReport_16725440065730/Dashboard1?:language=en-US&publish=yes&:display_count=n&:origin=viz_share_link](https://public.tableau.com/authoring/SalesReport_16974354318880/SalebyCityMap#1)

![step 0](https://github.com/Imrul2322/Data-Wrangling-and-Extract-information-using-Python-and-SQL/blob/main/Viz%20Assets/main%20dashboard.png "title")

Figure 1: Main Dashboard

![step 1](https://github.com/Imrul2322/Data-Wrangling-and-Extract-information-using-Python-and-SQL/blob/main/Viz%20Assets/country%20hover.png "title")

Figure 2: Country Hover

![step 2](https://github.com/Imrul2322/Data-Wrangling-and-Extract-information-using-Python-and-SQL/blob/main/Viz%20Assets/select%20country.png "title")

Figure 3: Country Select

![step 3](https://github.com/Imrul2322/Data-Wrangling-and-Extract-information-using-Python-and-SQL/blob/main/Viz%20Assets/expand%20city%20level.png "title")

Figure 4: Expand to city level

![step 4](https://github.com/Imrul2322/Data-Wrangling-and-Extract-information-using-Python-and-SQL/blob/main/Viz%20Assets/product%20cat%20hover.png "title")

Figure 5: Product Category Hover

## Conclusion
When it comes to choosing which country or region to invest in first in order to receive the best return, information gleaned from this project can be useful. Dashboards were developed to provide more detailed information. When appropriately presented to an organization's executive team, the data retrieved using SQL and Tableau can be highly helpful.
