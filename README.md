# Big Data Project: XKCD Comic Analytics Dashboard 

Author: Amanda de Moura
Institution: DHBW Stuttgart

### This project analyzes XKCD comics and creates a searchable database using MongoDB. It implements an automated ETL pipeline (Extract, Transform, Load) to process and store comic data. The project includes a simple HTML frontend for searching and querying the comic database. This project was created as part of a Big Data module and it runs on MacOS.

## 🗂 Table of Contents

- [Data Model](#data-model)
- [Project Structure](#project-structure)
- [Airflow Configuration](#airflow-configuration)
- [Features](#features)
- [Execution Instructions](#execution-instructions)

## Data Model

The project processes and stores comic data in MongoDB and makes them searchable through a Flask App UI.

## Project Structure

### The project structure is organized as follows:

```
├── airflow/
│   └── dags/
├── templates/
│   ├── comic.html
│   └── search_results.html
├── app.py
├── README.md
└── requirements.txt
```

### Explanation of the Structure


  
### Folder and File Descriptions

- **`airflow/`**  
Contains configurations and workflows for Apache Airflow, which orchestrates the ETL pipeline.

- **`dags/`**:  
  Houses Directed Acyclic Graph (DAG) files that define the workflow logic for fetching, processing, and storing XKCD comic data.

- **`templates/`**  
Stores HTML templates for the web frontend. These templates are rendered dynamically by the `app.py` Flask application.

- **`comic.html`**:  
  Template for displaying an individual comic with details such as title, number, and alt text.

- **`search_results.html`**:  
  Template for displaying the results of user searches, listing comics that match the search criteria.

- **`app.py`**  
The main application script. This Flask-based web app serves the HTML frontend and interacts with the database to fetch and display comics based on user input.

- **`README.md`**  
Documentation for the project, including an overview, setup instructions, usage guidelines, and additional notes.

- **`requirements.txt`**  
A list of Python dependencies required to run the project, including Flask, MongoDB drivers, and Airflow libraries.


## Airflow Configuration

The core of automation is the Airflow DAG defined in airflow/dags/xkcd_etl_dag.py.

The individual steps of the DAG and their dependencies are shown in the diagram and explained systematically below:



## Workflow Execution

### 1. Test and Clear HDFS  
- **Tasks**: `test_hdfs_task`, `clear_hdfs_task`  
  These tasks ensure that the HDFS environment is functional and cleared of any previous data before starting the workflow.

### 2. Test and Clear MongoDB  
- **Tasks**: `test_mongodb_task`, `clear_mongodb_task`  
  Ensures the MongoDB database is operational and clears any pre-existing data.

### 3. Fetch Comic Data  
- **Task**: `fetch_comics_task`  
  Downloads comic data from the XKCD API.

### 4. Store Fetched Data  
- **Task**: `store_fetched_data_task`  
  Saves the fetched data in a structured format to be used in subsequent steps.

### 5. Upload Raw Data to HDFS  
- **Task**: `upload_raw_data_to_hdfs_task`  
  Uploads the raw comic data to HDFS, partitioned by year.

### 6. Clean Data Within HDFS  
- **Task**: `clean_data_within_hdfs_task`  
  Cleans the raw data in HDFS by removing irrelevant fields and optimizing it for analysis.

### 7. Export to MongoDB  
- **Task**: `export_to_mongodb_task`  
  Transfers the cleaned data from HDFS to the MongoDB database.

---

This workflow efficiently processes and organizes XKCD comic data, providing a searchable database and key insights through a user-friendly frontend.


## Features

### **1. End-to-End ETL Pipeline**
- Fully automated ETL pipeline orchestrated using Apache Airflow.
- Fetches, processes, and stores XKCD comic data in a MongoDB database.

### **2. Searchable Comic Database**
- Provides a user-friendly interface to search for XKCD comics by keywords.
- Displays comics with title, number, alt text, and images.

### **3. Scalable Data Storage**
- Uses Hadoop's HDFS for raw data storage, partitioned by year.
- Optimized data cleaning and transformation within HDFS for scalability.

### **4. Interactive Web Frontend**
- Built with Flask to provide dynamic HTML pages.
- Includes pages for individual comic display and search results.

### **5. Data Insights**
- Cleans data to remove irrelevant fields and focus on meaningful attributes.
- Provides year-month breakdowns of comic publication data.


## Execution Instructions

Follow these steps to set up and execute the project locally.

---

### **1. Prerequisites**
Ensure the following tools are installed on your local machine:
- **Python** (version 3.7 or later)
- **Java** (for HDFS)
- **Node.js and npm** (optional, for frontend dependencies)
- **pip** (Python package manager)
- **MongoDB** (community or enterprise edition)
- **Airflow** (installed via pip)
- **Hadoop** (for HDFS)

---

### **2. Setting Up the Environment**

#### **2.1 Install Python Dependencies**
1. Create a virtual environment:
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
2. Install project dependencies:
   ```bash
   pip install -r requirements.txt
   ```
   
#### **2.2 Set Up Hadoop (HDFS)**

  Download Hadoop:
  ```bash
  wget https://downloads.apache.org/hadoop/common/hadoop-x.x.x/hadoop-x.x.x.tar.gz
  ```

  Extract and configure:
  ```bash
  tar -xvzf hadoop-x.x.x.tar.gz
  cd hadoop-x.x.x
  ```

  Update core-site.xml and hdfs-site.xml with the appropriate local configurations.

  Start HDFS:
  ```bash
  bin/hdfs namenode -format
  sbin/start-dfs.sh
  ```

  Verify HDFS is running by accessing `http://localhost:9870`.

#### **2.3 Set Up Airflow**

Install Airflow:
```bash
pip install apache-airflow
Initialize Airflow database:
airflow db init
```

Start the Airflow webserver and scheduler:
```
airflow webserver --port 8080 &
airflow scheduler &
```
Verify Airflow is running by accessing `http://localhost:8080`.

#### **2.4 Set Up MongoDB**

Download and install MongoDB:
```bash
brew tap mongodb/brew
brew install mongodb-community@6.0
```
Start MongoDB:
```bash
brew services start mongodb/brew/mongodb-community@6.0
```
Verify MongoDB is running by connecting with:
```bash
mongosh
```

#### **2.5 Set Up Flask**

The Flask app is included in app.py. Ensure all dependencies are installed from requirements.txt.
Run the Flask app:
```bash
python app.py
```
Access the web application at `http://localhost:5000`.

### **3. Executing the ETL Pipeline**

Place the DAG files in the Airflow DAGs directory:
```bash
cp airflow/dags/* ~/airflow/dags/
```
Trigger the DAG from the Airflow web UI or using the CLI:
```bash
airflow dags trigger <dag_id>
```

### **4. Accessing the Application**

- Use the Flask web interface to search for XKCD comics.  
- HDFS data can be browsed via `http://localhost:9870`.  
- MongoDB data can be inspected using the Mongo shell.


### **5. Notes**

- Ensure the services (HDFS, Airflow, MongoDB, Flask) are running in the background during execution.  
- Use the logs in Airflow and Flask for debugging any issues.
- This setup is for MacOS with an Apple Chip.
- Double check all configuration files and system variables (airflow.cfg, hdfs-site.xml, etc.).

