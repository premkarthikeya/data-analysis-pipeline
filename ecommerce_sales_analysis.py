import pandas as pd
import mysql.connector
import matplotlib.pyplot as plt
import os

file_name = "sales_data.csv"

if not os.path.exists(file_name):
    print("Error: sales_data.csv file not found.")
    exit()

df = pd.read_csv(file_name)

df = df.dropna(subset=["Order_ID", "Product", "Category", "Price", "Quantity"])
df["Price"] = pd.to_numeric(df["Price"], errors="coerce")
df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce")
df = df.dropna(subset=["Price", "Quantity"])
df["Order_Date"] = pd.to_datetime(df["Order_Date"], errors="coerce")

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="123456789",
    database="ecommerce_project"
)

cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS sales_data (
    Order_ID INT,
    Order_Date DATE,
    Product VARCHAR(100),
    Category VARCHAR(100),
    Price INT,
    Quantity INT,
    Customer_City VARCHAR(100)
)
""")


cursor.execute("DELETE FROM sales_data")

for _, row in df.iterrows():
    query = """
    INSERT INTO sales_data (Order_ID, Order_Date, Product, Category, Price, Quantity, Customer_City)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    values = (
        int(row["Order_ID"]),
        row["Order_Date"].date() if pd.notnull(row["Order_Date"]) else None,
        row["Product"],
        row["Category"],
        int(row["Price"]),
        int(row["Quantity"]),
        row["Customer_City"]
    )
    cursor.execute(query, values)

conn.commit()


query = "SELECT * FROM sales_data"
df = pd.read_sql(query, conn)


df["Revenue"] = df["Price"] * df["Quantity"]


total_revenue = df["Revenue"].sum()
total_orders = df["Order_ID"].nunique()
total_products = df["Product"].nunique()

product_quantity = df.groupby("Product")["Quantity"].sum().sort_values(ascending=False)
product_revenue = df.groupby("Product")["Revenue"].sum().sort_values(ascending=False)
category_revenue = df.groupby("Category")["Revenue"].sum().sort_values(ascending=False)
daily_revenue = df.groupby("Order_Date")["Revenue"].sum().sort_index()

top_selling_product = product_quantity.idxmax()
top_selling_quantity = product_quantity.max()

print("\n===== E-COMMERCE SALES ANALYSIS REPORT =====")
print(f"Total Revenue        : {total_revenue}")
print(f"Total Orders         : {total_orders}")
print(f"Unique Products      : {total_products}")
print(f"Top Selling Product  : {top_selling_product}")
print(f"Top Product Quantity : {top_selling_quantity}")

print("\nRevenue by Category:")
print(category_revenue)

print("\nRevenue by Product:")
print(product_revenue)

print("\nDaily Revenue:")
print(daily_revenue)


summary_df = pd.DataFrame({
    "Metric": [
        "Total Revenue",
        "Total Orders",
        "Unique Products",
        "Top Selling Product",
        "Top Product Quantity"
    ],
    "Value": [
        total_revenue,
        total_orders,
        total_products,
        top_selling_product,
        top_selling_quantity
    ]
})

summary_df.to_csv("sales_summary.csv", index=False)


product_revenue.plot(kind="bar", figsize=(8, 5))
plt.title("Revenue by Product")
plt.xlabel("Product")
plt.ylabel("Revenue")
plt.tight_layout()
plt.savefig("product_revenue_chart.png")
plt.show()

daily_revenue.plot(kind="line", marker="o", figsize=(8, 5))
plt.title("Daily Revenue Trend")
plt.xlabel("Date")
plt.ylabel("Revenue")
plt.tight_layout()
plt.savefig("daily_revenue_chart.png")
plt.show()

cursor.close()
conn.close()