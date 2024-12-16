from pymongo import MongoClient
import mysql.connector
import decimal
import time

# Establece conexión con la base de datos MySQL (Northwind)
mysql_conn = mysql.connector.connect(host="localhost", user="root", port=3306, password="12345", database="northwind")
cursor = mysql_conn.cursor(dictionary=True)  # Cursor para realizar consultas a la base de datos MySQL

# Establece conexión con el Cluster de mongodb
client = MongoClient("mongodb+srv://root:12345@clustergratis.oaelb.mongodb.net/")
mongo_db = client["Northwind"]  


# Función para migrar los datos de la tabla 'Categories' de MySQL a MongoDB
def migrate_categories():
    cursor.execute("SELECT * FROM Categories")  
    rows = cursor.fetchall()  
    collection = mongo_db["Categories"] 
    for row in rows:
        collection.insert_one({k: convert_decimal_to_float(v) for k, v in row.items()})  
    print("Migración de Categories Exitosa.")  

    # Función para convertir valores de tipo decimal a float
def convert_decimal_to_float(value):
    if isinstance(value, decimal.Decimal):
        return float(value)
    return value

# Función para migrar los datos de la tabla 'Customers' de MySQL a MongoDB
def migrate_customers():
    cursor.execute("SELECT * FROM Customers")  
    rows = cursor.fetchall() 
    collection = mongo_db["Customers"]  
    for row in rows:
        collection.insert_one({k: convert_decimal_to_float(v) for k, v in row.items()})  
    print("Migración de Customers Exitosa.")  


# Función para migrar los datos de la tabla 'Employees' de MySQL a MongoDB
def migrate_employees():
    cursor.execute("SELECT * FROM Employees") 
    rows = cursor.fetchall()  
    collection = mongo_db["Employees"]  
    for row in rows:
        collection.insert_one({k: convert_decimal_to_float(v) for k, v in row.items()}) 
    print("Migración de Employees Exitosa.") 

# Función para migrar los datos de la tabla 'Suppliers' de MySQL a MongoDB
def migrate_suppliers():
    cursor.execute("SELECT * FROM Suppliers")  
    rows = cursor.fetchall()  
    collection = mongo_db["Suppliers"]  
    for row in rows:
        collection.insert_one({k: convert_decimal_to_float(v) for k, v in row.items()}) 
    print("Migración de Suppliers Exitosa.") 

# Función para migrar datos de 'Orders' de MySQL a MongoDB
def migrate_orders():
    cursor.execute("SELECT * FROM Orders")  
    rows = cursor.fetchall()  
    collection = mongo_db["Orders"] 
    for row in rows:

        # Agrega los datos de Customer, Employee y OrderDetails en cada orden
        row["Customer"] = get_customer(row.pop("CustomerID"))
        row["Employee"] = get_employee(row.pop("EmployeeID"))
        row["OrderDetails"] = get_order_details(row["OrderID"])
        collection.insert_one({k: convert_decimal_to_float(v) for k, v in row.items()}) 
    print("Migración de Orders Exitosa.")  

    # Función para migrar los datos de 'Products' de MySQL a MongoDB
def migrate_products():
    cursor.execute("SELECT * FROM Products")  
    rows = cursor.fetchall()  
    collection = mongo_db["Products"] 

    for row in rows:
        try:
            # Asegurarse de que las unidades y precios sean convertibles a float
            unit = float(row.get("Unit", 0)) if isinstance(row.get("Unit"), (int, float, decimal.Decimal)) else 0
            price = float(row.get("Price", 0)) if isinstance(row.get("Price"), (int, float, decimal.Decimal)) else 0

            
            row["UnitPrice"] = unit * price

            # Convierte otros campos necesarios a su formato adecuado
            row["CategoryID"] = int(row["CategoryID"]) if row["CategoryID"] else None
            row["SupplierID"] = int(row["SupplierID"]) if row["SupplierID"] else None

            # Inserta el registro procesado en MongoDB
            collection.insert_one({k: convert_decimal_to_float(v) for k, v in row.items()})
        except Exception as e:
            print(f"Error al procesar el producto con ID {row.get('ProductID', 'Desconocido')}: {e}")

    print("Migración de Products Exitosa.")  


# Función para obtener los detalles de la orden a partir de su ID
def get_order_details(order_id):
    cursor.execute("SELECT * FROM OrderDetails WHERE OrderID = %s", (order_id,))
    order_details = cursor.fetchall() 
    return order_details if order_details else []  


# Función para obtener los datos del empleado a partir de su ID
def get_employee(employee_id):
    cursor.execute("SELECT * FROM Employees WHERE EmployeeID = %s", (employee_id,))
    employee = cursor.fetchone()  
    return employee if employee else None  

    # Función para obtener los datos del cliente a partir de su ID
def get_customer(customer_id):
    cursor.execute("SELECT * FROM Customers WHERE CustomerID = %s", (customer_id,))
    customer = cursor.fetchone() 
    return customer if customer else None 


# Función principal que ejecuta todas las migraciones
def main():
    try:
        print("Iniciando migración Northwind...") 
        
        migrate_customers()
        migrate_categories()  
        migrate_suppliers()  
        migrate_employees()  
        migrate_products() 
        migrate_orders()  
        print("Finalizo la Migración.")  
    except Exception as e:
        print(f"Ocurrio un error durante la migración: {e}") 
    finally:
        mysql_conn.close() 
        client.close()  

# Llama a la función main para iniciar el proceso de migración
if __name__ == "__main__":
    main()
