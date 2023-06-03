# Spark JDBC Read Write

This project contains a comprehensive codebase to read, write and update data from multiple JDBC sources and JDBC targets using pyspark.

Problem Statement:
Often in the daily lives of Data Engineers there is a need of code that reads data from JDBC sources such as Postgres, Oracle, SQLServer etc and write into the same.
However, there are multiple factors to consider while writing a production grade implementation: 
1. Code should not be limited to read and write data from only one of JDBC datasource
2. Code should be extensible. eg: Adding new JDBC sources should be easy and quick
3. All available performance parameters should be made available in the code for the job to be able to be tweaked at run time
4. Robust exception handling should be implemented to showcase exact errors at time of failure
5. Strong Test framework supporting above functionalities should be available

Most of the time, due to lack of timelines or expertise in early stages of career we usually create a pyspark code that contains everything in single file and is not modularized. It might also not cover all the edge scenarios and is susceptible to failures. The code structure might not organized in a standard way that it can be extensible. Lack of Test cases might lead to increased time in testing code manually.
The thought of solving such kinds of problems has led to creation this detailed codebase.

It would be great if anyone can provide suggestions and feedbacks; together we can work towards building a great reference spark codebase for budding Data Engineers.

Note: This is my first GitHub project. For starters, I am only limiting my content to read and write to JDBC using pyspark. Next plan of action is to write a generic CDC logic for any source and target type to determine changes in data and process them efficiently.

Happy Learning !

# TODO
1. Project Setup
2. Project Organization
3. How to Run the code
4. How to deploy code to a production like environment
5. How to build and publish the code to use it like a package in your organization's internal repository
6. How to run the test suite
