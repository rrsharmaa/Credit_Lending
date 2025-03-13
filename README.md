# THE DATA PIPELINE PROJECT

This project is a data pipeline application built with PySpark for data processing tasks. It ingests data from various sources, performs necessary transformations, and stores the processed data.


## 🚀 Getting Started

Follow these instructions to get the project up and running on your local machine, or Docker container, be it for development or testing.

### 📋 Prerequisites

Ensure you've got the following tools and environments set up:

- **Python 3.x**
- **pip**
- **virtualenv** (highly recommended)A

### 📁 data sample to run the code
- **Sample data is present to run the code** 


### 🔧 Installation & Execution


1. **Clone the repository**:

   ```bash
   git clone https://github.com/pgupta119/data_pipeline_abn.git
   ```

2. **Set up and activate your virtual environment**:

   ```bash
   python3 -m venv data_pipeline_abn 
   source data_pipeline_abn/bin/activate
   ```

3. **Install necessary packages**:

   ```bash
   pip3 install -r requirements.txt
   ```

4. **Execute the main script**:

   ```bash
   python3 main.py
   ```

   - **Output:** Files will be saved to the `data/output/` directory.

### 🔧 Docker Installation & Execution

1. **Change the directory to Dockerfile**:
2. **Pull apache/spark**:

   ```bash
    docker pull apache/spark 
   ```
3. **Build the docker image**:

   ```bash
    docker build -t data_pipeline_abn .
   ```
4. **Run the docker image**:


   ```bash
   # mounr the data from data folder to docker container so use the your path
   docker run --name my-spark-job -v add_your_directory/data_pipeline_abn/data:/app/data data_pipeline_abn




