# Fabric and Databricks Together

> This guide shows how Databricks clients can pull data from Fabric servers, and how Fabric clients can pull data from Databricks servers.

<br>

## Pull Data from Databricks to Fabric

<br>

1. **Identify a catalog in Databricks.**

   ![Image of Databricks Catalog](images/image-of-databricks-catalog.png)

---

<br>

2. **Navigate to Fabric and identify the workspace to land the Databricks catalog data in.**

   ![Fabric Workspace Image](images/databricks-west3-ws-workspace-image.png)

---

<br>

3. **Create a Mirrored Azure Databricks Catalog artifact.**

   ![Create Mirrored ADB Catalog](images/image-of-mirror-uc-artifact.png)

---

<br>

4. **Follow the wizard and create a connection if one does not already exist.**

---

<br>

5. **Choose a catalog (mine was `bronze` from earlier).**

   ![Image of Bronze Selected](images/image%20of%20bronze-selected.png)

---

<br>

6. **Choose the data you wish to bring in.**

   ![Image of Airline Table](images/image%20of%20airline%20table.png)

---

<br>

7. **Create names and choose a sensitivity label if desired.**

   ![Image of Sensitivity Label](images/image%20of%20sensitivity%20label.png)

---

<br>

8. **Wait until the shortcut is created.**

---

<br>

9. **Note that a shortcut has been created for the table, allowing zero-copy access to the data.**

   ![Image of Shortcut](images/Image%20of%20shortcut.png)

---

<br>

10. **Add OneLake security as desired.**

---

<br>

11. **Change to the query endpoint.**

    ![Image of Query Endpoint](images/Iamge%20of%20query%20endpoint.png)

---

<br>

12. **Query the data as desired.**

    ![Image of Querying Data](images/Image%20of%20querying%20data.png)

---

<br>

13. **Note that notebooks can also be used as desired.**

    ![Image of SQL Query Notebook](images/Image%20of%20SQL%20Query%20Notebook.png)
