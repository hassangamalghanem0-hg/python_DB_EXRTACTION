import pandas as pd
from sqlalchemy import create_engine


server = r'HASSAN\SQLEXPRESS'  


database = 'MyETLDB'  


conn_str = f"mssql+pyodbc://@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes"


engine = create_engine(conn_str)


df_employees = pd.read_sql("SELECT * FROM Employees", engine)
print("✅ Data Extracted from Employees table:")
print(df_employees)

df_orders = pd.read_sql("SELECT * FROM Orders", engine)
print("\n✅ Data Extracted from Orders table:")
print(df_orders)



# (Cleaning)

# Employees
df_employees = df_employees.drop_duplicates()
df_employees = df_employees.fillna({"Name": "Unknown", "Department": "Unknown", "Salary": 0})
df_employees = df_employees[df_employees["Salary"] >= 0]   

# Orders
df_orders = df_orders.drop_duplicates()
df_orders = df_orders.fillna({"CustomerName": "Unknown", "ProductName": "Unknown", "Quantity": 0, "Price": 0})
df_orders = df_orders[(df_orders["Quantity"] >= 0) & (df_orders["Price"] >= 0)] 


# (Standardization)

# lowercase
df_employees.columns = [col.lower() for col in df_employees.columns]
df_orders.columns = [col.lower() for col in df_orders.columns]

# Capitalized
df_employees["name"] = (df_employees["firstname"].str.strip().str.title() + " " +
                        df_employees["lastname"].str.strip().str.title())

df_orders["customername"] = df_orders["customername"].str.title()

# (histories)
df_employees["hiredate"] = pd.to_datetime(df_employees["hiredate"])
df_orders["orderdate"] = pd.to_datetime(df_orders["orderdate"])


# (Merging)


if "employeeid" in df_orders.columns:
    df_merged = pd.merge(df_orders, df_employees, on="employeeid", how="left")
else:
    
    df_merged = df_orders.copy()
    df_merged["employee_info"] = "Not linked (No EmployeeID column)"


#(final result)
print("\n✅ Cleaned Employees:")
print(df_employees.head())

print("\n✅ Cleaned Orders:")
print(df_orders.head())

print("\n✅ Merged Data (Orders + Employees):")
print(df_merged.head())




####################################################################################





import matplotlib.pyplot as plt

# Add total sales column
df_merged["TotalAmount"] = df_merged["quantity"] * df_merged["price"]

# 1) Total sales per employee
sales_per_employee = df_merged.groupby("name")["TotalAmount"].sum()
sales_per_employee.plot(kind="bar", figsize=(8,5), title="Total Sales per Employee")
plt.ylabel("Total Sales")
plt.xlabel("Employee")
plt.xticks(rotation=45)
plt.show()

# 2) Number of orders per employee
orders_per_employee = df_merged.groupby("name")["orderid"].count()
orders_per_employee.plot(kind="bar", figsize=(8,5), color="orange", title="Orders per Employee")
plt.ylabel("Number of Orders")
plt.xlabel("Employee")
plt.xticks(rotation=45)
plt.show()

# 3) Average salary per department
avg_salary_dept = df_employees.groupby("department")["salary"].mean()
avg_salary_dept.plot(kind="bar", figsize=(8,5), color="green", title="Avg Salary per Department")
plt.ylabel("Average Salary")
plt.xlabel("Department")
plt.show()

# 4) Sales trend over time
sales_over_time = df_merged.groupby("orderdate")["TotalAmount"].sum()
sales_over_time.plot(kind="line", marker="o", figsize=(8,5), title="Sales Over Time")
plt.ylabel("Total Sales")
plt.xlabel("Order Date")
plt.show()


#################################################


#load to DB

df_employees.to_sql("Cleaned_Employees", engine, if_exists="replace", index=False)

df_orders.to_sql("Cleaned_Orders", engine, if_exists="replace", index=False)

df_merged.to_sql("Merged_Data", engine, if_exists="replace", index=False)

print("✅ Cleaned data uploaded back to SQL Server successfully!")
