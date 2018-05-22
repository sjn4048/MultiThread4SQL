# MultiThread4SQL
A demo to enhance SQL performance by using multi-thread.

## Brief Introduction
Uses multi-thread to enhance the performance of SQL operations (namely SELECT and INSERT). After enhancement, the project can make operations 3-5 times faster.
(takes less than 30 seconds to insert 10,000,000 data into table on i5-8250U)

This is a bonus project for database course.

## Platform
.NET Framework + SQL Server (can be adapted to MySQL by slight changes)

## Usage
This is only a demo, fancy interface is not provided. Please change the code in main() function to set the arguments. The program will output the running time (including that of each thread) and errors met (if any)
