Download Link: https://assignmentchef.com/product/solved-metcs521-project-part-2
<br>
This assignment is the second of a mini project where you are demonstrating your understanding of the modules of this class.

The mini project is about building a small ETL program in python. Each pat of the project focuses on different portions / stages of the ETL process.

Assignment Background

Summarizing your acquired knowledge from module 5, we are going to focus on the L part of the ETL process. For reference, ETL stands for Extract, Transform, Load. We are going to focus here on Load.

Assignment Statement

<ul>

 <li>Save data in a database</li>

 <li>Read data from a database</li>

</ul>

You are required to supply <em>Last Name_First Name_Project_Part2.py and Last Name_First Name_create_dbs.py.  Please upload as a single zip file.</em>

<strong>Requirements:</strong>

Let’s create a place to store each dataset separately.

<ol>

 <li>Create a “create_dbs.py” file.</li>

 <li>In this file created in step 1, write python code using sqlite3 to:

  <ol>

   <li>Create a “baseball.db” database

    <ol>

     <li>Create 1 table named “baseball_stats” with the following columns:</li>

     <li>player_name</li>

     <li>games_played</li>

     <li>average</li>

     <li>salary</li>

    </ol></li>

  </ol></li>

</ol>

The “baseball.db” SQLite3 database should look like this:

”’Write a Python script to store baseball and stock data in SQLitedatabases.




The “baseball.db” SQLite3 database file should have one table:

Baseball_stats ======================== player_name text games_played int average real salary real




The “stocks.db” SQLite3 database file should have one table:

stock_stats ======================== company_name text ticker text country text price real exchange_rate real shares_outstanding real net_income real market_value real pe_ratio real”’




Baseball_stats

========================

player_name          text

games_played        int

average                  real

salary                     real




<ol start="3">

 <li>In this file created in step 1, write python code using sqlite3 to:

  <ol>

   <li>Create a “stocks.db” database:</li>

   <li>Create 1 table named “stock_stats”

    <ol>

     <li>company_name</li>

     <li>ticker</li>

     <li>exchange_country</li>

     <li>price</li>

     <li>exchange_rate</li>

     <li>share_outstanding</li>

     <li>net_income</li>

     <li>market_value_usd</li>

     <li>pe_ratio</li>

    </ol></li>

  </ol></li>

</ol>

The ” stocks.db” SQLite3 database should look like this:




stock_stats

========================

company_name       text

ticker                       text

country                    text

price                        real

exchange_rate         real

shares_outstanding real

net_income             real

market_value          real

pe_ratio                   real




<ol start="4">

 <li>Create AbstractDAO class. It should have the methods:

  <ol>

   <li>it should have 1 (instance) member: db_name</li>

   <li>insert_records(records) – Should raise the NotImplementedError</li>

   <li>select_all() – Should raise the NotImplementedError</li>

   <li>connect():

    <ol>

     <li>connect to the database identified by db_name</li>

     <li>returns the created connection</li>

    </ol></li>

   <li>Create BaseballStatsDAO class

    <ol>

     <li>Class should inherit AbstractDAO</li>

    </ol></li>

  </ol></li>

</ol>




<ol>

 <li>Class should implement the methods listed:

  <ol>

   <li>insert_records:

    <ol>

     <li>takes a list of records as parameter ( BaseballStatsDAO takes BaseballStatRecord)</li>

     <li>call the method connect()</li>

     <li>using the returned connection, create a cursor.</li>

     <li>For each record in the list, write and execute an INSERT INTO statement to save the record’s information to the correct table.</li>

    </ol></li>

  </ol></li>

</ol>

Example for baseball: cursor.execute(“INSERT INTO baseball_stats VALUES ( ?, ? , ? , ?)”, (name, number_games_played, avg, salary))

<ol start="5">

 <li>Commit the connection</li>

 <li>Close the connection</li>

</ol>

<ol start="2">

 <li>select_all

  <ol>

   <li>call the method connect()</li>

   <li>using the returned connection, create a cursor.</li>

   <li>create an empty deque to hold the records in memory</li>

   <li>write and execute a SELECT statement to get all the records of the table for the DAO</li>

  </ol></li>

</ol>

Example for baseball: cursor.execute(“SELECT player_name, games_played, average, salary FROM baseball_stats;”)

<ol start="5">

 <li>For each row fetched, iterate with a for loop over the result of your select command

  <ol>

   <li>Create a new record (BaseballStatRecord)</li>

   <li>Add the record to the deque</li>

  </ol></li>

 <li>Close the connection</li>

 <li>Return the deque</li>

</ol>

<ol start="6">

 <li>Create StockStatsDAO class

  <ol>

   <li>Class should inherit AbstractDAO</li>

   <li>Class should implement the methods listed:

    <ol>

     <li>insert_records:

      <ol>

       <li>takes a list of records as parameter (StockStatsDAO takes StockStatRecord)</li>

       <li>invokes the method connect()</li>

       <li>using the returned connection, create a cursor.</li>

       <li>For each record in the list, write and execute an INSERT INTO statement to save the record’s information to the correct table.</li>

      </ol></li>

    </ol></li>

  </ol></li>

</ol>

Example for stocks: see insert statement for baseball stats

<ol start="5">

 <li>Commit the connection</li>

 <li>Close the connection</li>

</ol>

<ol start="2">

 <li>select_all

  <ol>

   <li>invokes the method connect()</li>

   <li>using the returned connection, create a cursor.</li>

   <li>create an empty deque to hold the records in memory</li>

   <li>write and execute a SELECT statement to get all the records of the table for the DAO</li>

  </ol></li>

</ol>

Example for stocks: refer to select statement for Baseball

<ol start="5">

 <li>For each row fetched, iterate with a for loop over the result of your execute:

  <ol>

   <li>Create a new record (StockStatRecord)</li>

   <li>Add the record to the deque</li>

  </ol></li>

 <li>Close the connection</li>

 <li>Return the deque</li>

</ol>




Use the code written for Project Part 1.  If needed, a partial solution can be provided by your facilitator.  This will only be done on a case by case basis and only in extreme circumstances.  A 10-point deduction will be made if a solution is needed.

This section loads the records into the correct database using the classes from Project Part1

<ol start="7">

 <li>load MLB2008.csv using the BaseballCSVReader</li>

 <li>load StockValuations.csv using the StocksCSVReader</li>

 <li>Instantiate a new DAO instance for BaseballStats</li>

 <li>Instantiate a new DAO instance for StocksStats</li>

 <li>Insert the loaded records into baseball database using Baseball DAO’s insert_records.</li>

 <li>Insert the loaded records into stocks database using Stocks DAO’s insert_records.</li>

</ol>




Awesome! Now we have data in the database. Let’s use it!




Stocks stats:

<ol start="13">

 <li>Using the instance of StockStatsDAO select_all the records

  <ol>

   <li>Calculate and print the number of tickers by exchange_country using a dictionary.</li>

  </ol></li>

</ol>

Baseball stats:

<ol start="14">

 <li>Using the instance of BaseballStatsDAO select_all the records

  <ol>

   <li>Compute the average salary and enter into a dictionary by batting average</li>

   <li>Print the dictionary formatting the salary to 2 decimal places.</li>

  </ol></li>

</ol>




NOTE: use round(record.avg, 3)) The rounding needs to happen at average time

<h1><a name="_Toc475886609"></a>Code/Comment Format</h1>




Good code includes well named variables that are consistent from the beginning to the end of the program.  Naming of objects should be self-explanatory.  For instance, iterator_for_noun_list is much better than i.




Every program consists of a sequence of paragraphs, each of which has objectives, and which builds on the previous paragraphs. We are mostly interested in objectives that are valid at the end of the program so we can verify the program’s design. The following is a preferred form for such paragraph headings.  The # sign is adequate when the comment is a single line.




#This is an in-line comment – used to document the code for you, or anyone else, that intends

#To extend the code




In-line comments are helpful when one has to go back to the code 6 months later to make changes.




For doc strings, python allows the use of triple quotes.  The triple quotes can be either single or double quotes.  A doc sting is generally used as user documentation.  It does not need to include details of the implementation of the program, but instead it provides documentation as how to use the API for the program (input, output etc.)




For example:




“””

This is an example of a doc string

It allows multiple lines within the string.




“””

‘’’

This is an example of a doc string

It allows multiple lines within the string.




‘’’

This becomes significant when using functions, classes etc. as the triple quotes help to self-document the parameters and return values of the function.




<h1><a name="_Toc475886610"></a>Sample Output: The output is a sample and is not complete</h1>




0.083 400,000.00

0.130 600,000.00

0.132 1,500,000.00

0.147 6,333,333.00

0.158 14,726,910.00

0.159 3,850,000.00




IE 1

CA 10

ZA 1

JP 37

NO 2

SE 7








