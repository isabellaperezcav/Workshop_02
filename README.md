## 📦 Workshop 02 — ETL Pipeline with Apache Airflow

This project implements a **complete ETL pipeline** using Apache Airflow. It extracts data from three different sources (an API, a CSV file, and a database), transforms and merges the information, and finally loads the results into both a database and Google Drive. As a final step, visualizations are generated based on the data stored in the database.

---

## Technologies Used

- Python 3.9+
- Apache Airflow
- PostgreSQL (you can use another relational database)
- Google Drive API
- Ubuntu 22.04.0
- Power BI  
- Jupyter Notebook (for exploratory data analysis)

---

## 📁 Folder Structure

```bash
WORKSHO_002/
│
├── API/                          
│
├── dags/
│   └── dags_conections/        # Airflow connection settings
│   │   ├── __init__.py
│   │   └── dags_conections.py
│   │
│   ├── data/                        
│   │
│   ├── etls/                    # ETL pipeline scripts
│   │   ├── __init__.py
│   │   ├── dag_api_extract.py     # API extraction
│   │   ├── grammy_etl.py          # Grammy ETL process
│   │   ├── merge_load_data.py     # Data merging and final load
│   │   └── spotify_etl.py         # Spotify ETL process
│
├── Grammy/                      # Grammy data analysis and loading
│   ├── caragr_dts.ipynb
│   ├── conexion_db.py           # DB connection
│   ├── EDA_grammy.ipynb         # Exploratory data analysis
│   └── the_grammy_awards.csv    # Original dataset loaded into PostgreSQL
│
├── Spotify/                     # Spotify dataset and analysis
│   ├── EDA_spotify.ipynb
│   └── spotify_dataset.csv
│
├── pdf/                         # Documentation 
├── .gitignore                   # Sensitive files ignored
├── README.md
└── requirements.txt             # Required libraries
```

---

## Prerequisites

1. Python 3.9 or higher  
2. Airflow (recommended installation for Windows: Docker → [Official guide](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html))  
3. Google account with Google Drive access and API credentials (see below)  
4. A relational database like PostgreSQL (local or remote)  

---

## 🔐 `.env` File Configuration

Create a `.env` file at the root of the project with the following structure:

```env
DB_NAME=db
DB_USER=user
DB_PASSWORD=password
DB_HOST=host
DB_PORT=123
GOOGLE_APPLICATION_CREDENTIALS=/full/path/to/credentials/google_credentials.json
```

---

## 🔑 Google Drive API — Setup

1. Go to [Google Cloud Console](https://console.cloud.google.com/)  
2. Create a new project  
3. Enable the **Google Drive API**  
4. Create a key for a **service account**  
5. Download the `.json` key file and place it in `/worksho_002`  
6. Share your Google Drive folder with the service account's email  

---

## Data Sources Used

- 🎵 [Spotify Tracks Dataset](https://www.kaggle.com/datasets/maharshipandya/-spotify-tracks-dataset)  
- 🏆 [Grammy Awards Dataset](https://www.kaggle.com/datasets/unanimad/grammy-awards)  
- 🌐 [last.fm](https://www.last.fm/api/intro)  

---

## Running the Pipeline

### 1. Clone the repository

```bash
git clone https://github.com/isabellaperezcav/Workshop_02.git
cd Workshop_02
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Set up your environment

- Add the `.env` file  
- Place the Google API credentials file  
- Check file paths in the code and update them if needed  

### 4. Start Airflow

```bash
airflow standalone
```

### 5. Access Airflow

Visit `http://localhost:8080`  
Username: `admin`  
Password: printed in terminal under `standalone_admin_password`

---

## Project DAG

The main DAG performs the following steps:

1. **Extract**  
   - Spotify data from a CSV file  
   - Grammy data from the database  
   - Data from an external API  

2. **Transform**  
   - Clean datasets, merge them, create new columns  

3. **Load**  
   - Insert data into a database  
   - Export final results as CSV to Google Drive  

---

## Visualizations

Found in the `dash` folder

---

## Additional Resources

- [Official Airflow Documentation](https://airflow.apache.org/docs/)  
- [Guide to Connect Airflow with Google Drive](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/transfer/local_to_drive.html)  
- [Kaggle Spotify Dataset](https://www.kaggle.com/datasets/maharshipandya/-spotify-tracks-dataset)  
- [Kaggle Grammy Dataset](https://www.kaggle.com/datasets/unanimad/grammy-awards)

---
